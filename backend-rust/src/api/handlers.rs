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
use sqlx::{FromRow, PgPool, Row};
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
    pub data_source: &'static str,
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

#[derive(Debug, Serialize, FromRow)]
pub struct DeliveryDto {
    pub delivery_id: i64,
    pub driver_id: String,
    pub driver_name: String,
    pub zone: String,
    pub package_code: String,
    pub status: String,
    pub scheduled_time: DateTime<Utc>,
    pub delivered_time: Option<DateTime<Utc>>,
    pub distance_km: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct RouteOptimizationDto {
    pub optimization_id: i64,
    pub target_date: NaiveDate,
    pub driver_id: String,
    pub driver_name: String,
    pub assigned_zone: String,
    pub recommended_waypoints: serde_json::Value,
    pub estimated_saving_km: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct AlertQuery {
    pub limit: Option<usize>,
}

/// Consulta PostgreSQL para obtener la ultima posicion de cada repartidor (`drivers` + `LATERAL gps_telemetry`)
pub async fn fetch_fleet_from_postgres(pool: &PgPool) -> Result<Vec<GpsTelemetryPayload>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            d.driver_id,
            d.driver_name,
            d.zone,
            d.assigned_lat,
            d.assigned_lon,
            d.delivery_lat,
            d.delivery_lon,
            COALESCE(t.latitude, d.assigned_lat) AS latitude,
            COALESCE(t.longitude, d.assigned_lon) AS longitude,
            COALESCE(t.speed_kmh, 35.0) AS speed_kmh,
            COALESCE(t.delivery_status, d.status) AS delivery_status,
            COALESCE(t.deviation_meters, 0.0) AS deviation_meters,
            COALESCE(t.recorded_at, d.updated_at) AS recorded_at
        FROM drivers d
        LEFT JOIN LATERAL (
            SELECT latitude, longitude, speed_kmh, delivery_status, deviation_meters, recorded_at
            FROM gps_telemetry gt
            WHERE gt.driver_id = d.driver_id
            ORDER BY gt.recorded_at DESC
            LIMIT 1
        ) t ON TRUE
        ORDER BY d.driver_id ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let vehicles = rows
        .into_iter()
        .map(|r| {
            let speed_kmh: f64 = r.get("speed_kmh");
            let status: String = r.get("delivery_status");
            let idle_mins = if speed_kmh <= 0.5 && status == "IDLE_STOPPED" {
                12.0
            } else {
                0.0
            };

            GpsTelemetryPayload {
                driver_id: r.get("driver_id"),
                driver_name: Some(r.get("driver_name")),
                zone: Some(r.get("zone")),
                latitude: r.get("latitude"),
                longitude: r.get("longitude"),
                speed_kmh,
                delivery_status: status,
                assigned_lat: Some(r.get("assigned_lat")),
                assigned_lon: Some(r.get("assigned_lon")),
                delivery_lat: Some(r.get("delivery_lat")),
                delivery_lon: Some(r.get("delivery_lon")),
                deviation_meters: Some(r.get("deviation_meters")),
                idle_minutes: Some(idle_mins),
                timestamp: r.get("recorded_at"),
            }
        })
        .collect();

    Ok(vehicles)
}

/// Consulta PostgreSQL para obtener las alertas de ruta (`route_alerts` JOIN `drivers`)
pub async fn fetch_alerts_from_postgres(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RouteAlertPayload>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            ra.alert_id::TEXT AS alert_id,
            ra.driver_id,
            COALESCE(d.driver_name, ra.driver_id) AS driver_name,
            COALESCE(d.zone, 'Metropolitana') AS zone,
            ra.alert_type,
            ra.severity,
            ra.message,
            ra.latitude,
            ra.longitude,
            ra.metric_value,
            ra.created_at
        FROM route_alerts ra
        LEFT JOIN drivers d ON d.driver_id = ra.driver_id
        ORDER BY ra.created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let alerts = rows
        .into_iter()
        .map(|r| RouteAlertPayload {
            alert_id: r.get("alert_id"),
            driver_id: r.get("driver_id"),
            driver_name: r.get("driver_name"),
            zone: r.get("zone"),
            alert_type: r.get("alert_type"),
            severity: r.get("severity"),
            message: r.get("message"),
            latitude: r.get("latitude"),
            longitude: r.get("longitude"),
            metric_value: r.get("metric_value"),
            created_at: r.get("created_at"),
        })
        .collect();

    Ok(alerts)
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

/// GET `/api/fleet/state`: Consulta el estado actual de la flotilla y alertas directamente desde PostgreSQL (`drivers`, `gps_telemetry` y `route_alerts`)
pub async fn get_fleet_state_handler(State(state): State<AppState>) -> Json<FleetStatusResponse> {
    if let Some(ref pool) = state.db_pool {
        if let (Ok(db_vehicles), Ok(db_alerts)) = (
            fetch_fleet_from_postgres(pool).await,
            fetch_alerts_from_postgres(pool, 50).await,
        ) {
            return Json(FleetStatusResponse {
                simulator_active: state.simulator_running.load(Ordering::Relaxed),
                active_drivers: db_vehicles.len(),
                total_recent_alerts: db_alerts.len(),
                data_source: "postgresql",
                vehicles: db_vehicles,
                alerts: db_alerts,
            });
        }
    }

    // Fallback a cache en memoria solo si PostgreSQL no esta disponible
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
        data_source: "in_memory_fallback",
        vehicles,
        alerts,
    })
}

/// GET `/api/alerts`: Consulta las alertas de desvío (>500m) e inactividad (>10 min) desde la tabla `route_alerts` en PostgreSQL
pub async fn get_alerts_handler(
    State(state): State<AppState>,
    Query(q): Query<AlertQuery>,
) -> Json<Vec<RouteAlertPayload>> {
    let limit = q.limit.unwrap_or(25).clamp(1, 100);

    if let Some(ref pool) = state.db_pool {
        if let Ok(db_alerts) = fetch_alerts_from_postgres(pool, limit as i64).await {
            return Json(db_alerts);
        }
    }

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

/// GET `/api/kpis`: Obtiene los KPIs diarios por zona desde la tabla `daily_zone_kpis` en PostgreSQL
pub async fn get_daily_kpis_handler(
    State(state): State<AppState>,
) -> (StatusCode, Json<Vec<DailyZoneKpiDto>>) {
    if let Some(ref pool) = state.db_pool {
        match sqlx::query_as::<_, DailyZoneKpiDto>(
            "SELECT report_date, zone, total_deliveries, successful_deliveries, on_time_rate, total_alerts,
                    CAST(driver_payout_usd AS DOUBLE PRECISION) AS driver_payout_usd, computed_at
             FROM daily_zone_kpis
             ORDER BY report_date DESC, zone ASC
             LIMIT 15",
        )
        .fetch_all(pool)
        .await
        {
            Ok(rows) => return (StatusCode::OK, Json(rows)),
            Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(vec![])),
        }
    }

    (StatusCode::SERVICE_UNAVAILABLE, Json(vec![]))
}

/// GET `/api/deliveries`: Consulta el historial de entregas desde la tabla `deliveries` JOIN `drivers` en PostgreSQL
pub async fn get_deliveries_handler(
    State(state): State<AppState>,
) -> (StatusCode, Json<Vec<DeliveryDto>>) {
    if let Some(ref pool) = state.db_pool {
        if let Ok(rows) = sqlx::query_as::<_, DeliveryDto>(
            r#"
            SELECT
                del.delivery_id,
                del.driver_id,
                COALESCE(d.driver_name, del.driver_id) AS driver_name,
                del.zone,
                del.package_code,
                del.status,
                del.scheduled_time,
                del.delivered_time,
                del.distance_km,
                del.created_at
            FROM deliveries del
            LEFT JOIN drivers d ON d.driver_id = del.driver_id
            ORDER BY del.created_at DESC
            LIMIT 50
            "#,
        )
        .fetch_all(pool)
        .await
        {
            return (StatusCode::OK, Json(rows));
        }
    }
    (StatusCode::SERVICE_UNAVAILABLE, Json(vec![]))
}

/// GET `/api/optimizations`: Consulta los planes de optimización de rutas generados por Airflow en la tabla `route_optimizations` de PostgreSQL
pub async fn get_route_optimizations_handler(
    State(state): State<AppState>,
) -> (StatusCode, Json<Vec<RouteOptimizationDto>>) {
    if let Some(ref pool) = state.db_pool {
        if let Ok(rows) = sqlx::query(
            r#"
            SELECT
                ro.optimization_id,
                ro.target_date,
                ro.driver_id,
                COALESCE(d.driver_name, ro.driver_id) AS driver_name,
                ro.assigned_zone,
                ro.recommended_waypoints::TEXT AS waypoints_json,
                ro.estimated_saving_km,
                ro.created_at
            FROM route_optimizations ro
            LEFT JOIN drivers d ON d.driver_id = ro.driver_id
            ORDER BY ro.target_date DESC, ro.driver_id ASC
            LIMIT 30
            "#,
        )
        .fetch_all(pool)
        .await
        {
            let list = rows
                .into_iter()
                .map(|r| {
                    let wp_str: String = r.get("waypoints_json");
                    let wp_val = serde_json::from_str(&wp_str).unwrap_or(serde_json::json!([]));
                    RouteOptimizationDto {
                        optimization_id: r.get("optimization_id"),
                        target_date: r.get("target_date"),
                        driver_id: r.get("driver_id"),
                        driver_name: r.get("driver_name"),
                        assigned_zone: r.get("assigned_zone"),
                        recommended_waypoints: wp_val,
                        estimated_saving_km: r.get("estimated_saving_km"),
                        created_at: r.get("created_at"),
                    }
                })
                .collect();
            return (StatusCode::OK, Json(list));
        }
    }
    (StatusCode::SERVICE_UNAVAILABLE, Json(vec![]))
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
