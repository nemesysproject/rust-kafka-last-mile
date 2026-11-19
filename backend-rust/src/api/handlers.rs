use crate::kafka::consumer::{
    evaluate_geofence_and_rules, DriverIdleTracker, DriverRouteReference, LiveStreamEvent,
};
use crate::kafka::producer::{GpsTelemetryPayload, KafkaProducerService, RouteAlertPayload};
use crate::simulator::fleet_gen::simulate_fleet_step;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

#[derive(Clone)]
pub struct AppState {
    pub producer: KafkaProducerService,
    pub sse_tx: broadcast::Sender<LiveStreamEvent>,
    pub routes: Arc<RwLock<HashMap<String, DriverRouteReference>>>,
    pub idle_trackers: Arc<RwLock<HashMap<String, DriverIdleTracker>>>,
    pub latest_fleet: Arc<RwLock<HashMap<String, GpsTelemetryPayload>>>,
    pub recent_alerts: Arc<RwLock<Vec<RouteAlertPayload>>>,
    pub simulator_running: Arc<AtomicBool>,
    pub db_pool: Option<PgPool>,
    pub api_key: Option<String>,
    pub max_tracked_drivers: usize,
}

#[derive(Debug, Deserialize)]
pub struct IngestTelemetryRequest {
    pub driver_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub speed_kmh: f64,
    pub delivery_status: Option<String>,
    pub idle_minutes: Option<f64>,
}

impl IngestTelemetryRequest {
    /// Valida rangos geograficos, velocidades finitas y formato seguro de driver_id
    pub fn validate(&self) -> Result<(), &'static str> {
        let id = self.driver_id.trim();
        if id.is_empty() || id.len() > 32 {
            return Err("driver_id debe tener entre 1 y 32 caracteres");
        }
        if !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err("driver_id contiene caracteres no permitidos");
        }
        if !self.latitude.is_finite() || self.latitude < -90.0 || self.latitude > 90.0 {
            return Err("latitude fuera de rango valido (-90.0 a 90.0)");
        }
        if !self.longitude.is_finite() || self.longitude < -180.0 || self.longitude > 180.0 {
            return Err("longitude fuera de rango valido (-180.0 a 180.0)");
        }
        if !self.speed_kmh.is_finite() || self.speed_kmh < 0.0 || self.speed_kmh > 250.0 {
            return Err("speed_kmh fuera de rango valido (0.0 a 250.0 km/h)");
        }
        if let Some(idle) = self.idle_minutes {
            if !idle.is_finite() || idle < 0.0 || idle > 1440.0 {
                return Err("idle_minutes fuera de rango valido (0.0 a 1440.0)");
            }
        }
        Ok(())
    }
}

/// Verifica el header `X-API-Key` si se configuro `SWIFTROUTE_API_KEY` en el entorno
fn verify_api_key(headers: &HeaderMap, expected_key: &Option<String>) -> bool {
    let Some(ref expected) = expected_key else {
        return true;
    };
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|provided| provided == expected)
        .unwrap_or(false)
}

#[derive(Debug, Deserialize)]
pub struct SimulationTriggerRequest {
    pub mode: Option<String>, // "normal" | "deviation" | "idle" | "toggle_auto"
    pub burst_count: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct FleetStatusResponse {
    pub simulator_active: bool,
    pub active_drivers: usize,
    pub total_recent_alerts: usize,
    pub vehicles: Vec<GpsTelemetryPayload>,
    pub alerts: Vec<RouteAlertPayload>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct DailyZoneKpiDto {
    pub report_date: NaiveDate,
    pub zone: String,
    pub total_deliveries: i32,
    pub successful_deliveries: i32,
    pub on_time_rate: f64,
    pub total_alerts: i32,
    pub driver_payout_usd: f64,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct AlertQuery {
    pub limit: Option<usize>,
}

/// Healthcheck del servicio Rust
pub async fn health_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "UP",
        "service": "swiftroute-rust-core-api",
        "simulator_running": state.simulator_running.load(Ordering::Relaxed),
        "postgres_connected": state.db_pool.is_some(),
        "api_key_enforced": state.api_key.is_some(),
        "timestamp": Utc::now()
    }))
}

/// POST `/api/telemetry`: Recibe un paquete de telemetría GPS validado, lo publica en Kafka y evalúa geocercas
pub async fn ingest_telemetry_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<IngestTelemetryRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !verify_api_key(&headers, &state.api_key) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "accepted": false,
                "error": "X-API-Key invalida o ausente"
            })),
        );
    }

    if let Err(validation_err) = req.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "accepted": false,
                "error": validation_err
            })),
        );
    }

    // Verificar limite de cardinalidad en memoria para evitar ataques DoS por inundacion de driver_ids
    {
        let fleet = state.latest_fleet.read().await;
        if fleet.len() >= state.max_tracked_drivers && !fleet.contains_key(&req.driver_id) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "accepted": false,
                    "error": "Limite maximo de unidades rastreadas alcanzado"
                })),
            );
        }
    }

    let payload = GpsTelemetryPayload {
        driver_id: req.driver_id.clone(),
        driver_name: None,
        zone: None,
        latitude: req.latitude,
        longitude: req.longitude,
        speed_kmh: req.speed_kmh,
        delivery_status: req
            .delivery_status
            .unwrap_or_else(|| "IN_TRANSIT".to_string()),
        assigned_lat: None,
        assigned_lon: None,
        delivery_lat: None,
        delivery_lon: None,
        deviation_meters: None,
        idle_minutes: req.idle_minutes,
        timestamp: Utc::now(),
    };

    let published_to_kafka = state.producer.publish_telemetry(&payload).await.is_ok();
    if !published_to_kafka {
        evaluate_geofence_and_rules(
            payload.clone(),
            &state.routes,
            &state.idle_trackers,
            &state.latest_fleet,
            &state.recent_alerts,
            &state.sse_tx,
            &state.producer,
            &state.db_pool,
        )
        .await;
    }

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "accepted": true,
            "driver_id": req.driver_id,
            "kafka_broker_published": published_to_kafka,
            "timestamp": payload.timestamp
        })),
    )
}

/// GET `/api/fleet/state`: Snapshot actual de todos los vehículos y alertas recientes
pub async fn get_fleet_state_handler(State(state): State<AppState>) -> Json<FleetStatusResponse> {
    let vehicles: Vec<GpsTelemetryPayload> = {
        let guard = state.latest_fleet.read().await;
        let mut list: Vec<_> = guard.values().cloned().collect();
        list.sort_by(|a, b| a.driver_id.cmp(&b.driver_id));
        list
    };

    let alerts: Vec<RouteAlertPayload> = {
        let guard = state.recent_alerts.read().await;
        guard.clone()
    };

    Json(FleetStatusResponse {
        simulator_active: state.simulator_running.load(Ordering::Relaxed),
        active_drivers: vehicles.len(),
        total_recent_alerts: alerts.len(),
        vehicles,
        alerts,
    })
}

/// GET `/api/alerts`: Lista de alertas de desvío (>500m) e inactividad (>10 min)
pub async fn get_alerts_handler(
    State(state): State<AppState>,
    Query(q): Query<AlertQuery>,
) -> Json<Vec<RouteAlertPayload>> {
    let limit = q.limit.unwrap_or(25).clamp(1, 100);
    let guard = state.recent_alerts.read().await;
    let items: Vec<_> = guard.iter().take(limit).cloned().collect();
    Json(items)
}

/// POST `/api/simulator/trigger`: Dispara una ráfaga de carga o fuerza anomalías (Desvío >500m / Parada >10m)
pub async fn trigger_simulator_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SimulationTriggerRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !verify_api_key(&headers, &state.api_key) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "status": "rejected",
                "error": "X-API-Key invalida o ausente"
            })),
        );
    }

    let mode = req.mode.as_deref().unwrap_or("normal");
    if !matches!(mode, "normal" | "deviation" | "idle" | "toggle_auto") {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "rejected",
                "error": "Modo no permitido. Valores validos: normal, deviation, idle, toggle_auto"
            })),
        );
    }

    if mode == "toggle_auto" {
        let current = state.simulator_running.load(Ordering::Relaxed);
        state.simulator_running.store(!current, Ordering::Relaxed);
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "toggled",
                "simulator_active": !current
            })),
        );
    }

    let bursts = req.burst_count.unwrap_or(1).clamp(1, 20);
    let mut total_events = 0;

    for _ in 0..bursts {
        let generated = simulate_fleet_step(
            Some(mode),
            &state.routes,
            &state.idle_trackers,
            &state.latest_fleet,
            &state.recent_alerts,
            &state.sse_tx,
            &state.producer,
            &state.db_pool,
        )
        .await;
        total_events += generated.len();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "executed",
            "mode": mode,
            "bursts": bursts,
            "events_emitted": total_events,
            "timestamp": Utc::now()
        })),
    )
}

/// GET `/api/kpis`: Obtiene los KPIs diarios por zona desde PostgreSQL (o datos semilla en fallback)
pub async fn get_daily_kpis_handler(State(state): State<AppState>) -> Json<Vec<DailyZoneKpiDto>> {
    if let Some(ref pool) = state.db_pool {
        if let Ok(rows) = sqlx::query_as::<_, DailyZoneKpiDto>(
            "SELECT report_date, zone, total_deliveries, successful_deliveries, on_time_rate, total_alerts,
                    CAST(driver_payout_usd AS DOUBLE PRECISION) AS driver_payout_usd, computed_at
             FROM daily_zone_kpis
             ORDER BY report_date DESC, zone ASC
             LIMIT 15",
        )
        .fetch_all(pool)
        .await
        {
            if !rows.is_empty() {
                return Json(rows);
            }
        }
    }

    // Fallback en caso de ejecución sin base de datos
    let today = Utc::now().date_naive();
    Json(vec![
        DailyZoneKpiDto {
            report_date: today,
            zone: "Norte-Industrial".to_string(),
            total_deliveries: 24,
            successful_deliveries: 23,
            on_time_rate: 95.8,
            total_alerts: 1,
            driver_payout_usd: 145.50,
            computed_at: Utc::now(),
        },
        DailyZoneKpiDto {
            report_date: today,
            zone: "Centro-Historico".to_string(),
            total_deliveries: 31,
            successful_deliveries: 28,
            on_time_rate: 90.3,
            total_alerts: 3,
            driver_payout_usd: 172.00,
            computed_at: Utc::now(),
        },
        DailyZoneKpiDto {
            report_date: today,
            zone: "Poniente-SantaFe".to_string(),
            total_deliveries: 22,
            successful_deliveries: 22,
            on_time_rate: 100.0,
            total_alerts: 0,
            driver_payout_usd: 160.00,
            computed_at: Utc::now(),
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_telemetry_request_valid() {
        let req = IngestTelemetryRequest {
            driver_id: "DRV-001".to_string(),
            latitude: 19.4326,
            longitude: -99.1332,
            speed_kmh: 45.0,
            delivery_status: Some("IN_TRANSIT".to_string()),
            idle_minutes: Some(0.0),
        };
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_validate_telemetry_rejects_invalid_coordinates_and_injection() {
        let bad_lat = IngestTelemetryRequest {
            driver_id: "DRV-001".to_string(),
            latitude: 125.0,
            longitude: -99.1332,
            speed_kmh: 40.0,
            delivery_status: None,
            idle_minutes: None,
        };
        assert!(bad_lat.validate().is_err());

        let bad_id = IngestTelemetryRequest {
            driver_id: "DRV-001'; DROP TABLE drivers;--".to_string(),
            latitude: 19.4326,
            longitude: -99.1332,
            speed_kmh: 40.0,
            delivery_status: None,
            idle_minutes: None,
        };
        assert!(bad_id.validate().is_err());
    }
}
