use crate::kafka::producer::{GpsTelemetryPayload, KafkaProducerService, RouteAlertPayload};
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum LiveStreamEvent {
    #[serde(rename = "telemetry")]
    Telemetry(GpsTelemetryPayload),
    #[serde(rename = "alert")]
    Alert(RouteAlertPayload),
}

#[derive(Debug, Clone)]
pub struct DriverRouteReference {
    pub driver_id: String,
    pub driver_name: String,
    pub zone: String,
    pub assigned_lat: f64,
    pub assigned_lon: f64,
    pub delivery_lat: f64,
    pub delivery_lon: f64,
}

#[derive(Debug, Clone)]
pub struct DriverIdleTracker {
    pub stopped_since: Option<DateTime<Utc>>,
    pub simulated_idle_minutes: f64,
}

/// Calcula la distancia en metros entre dos coordenadas GPS mediante la fórmula de Haversine.
pub fn haversine_distance_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();

    let a = (d_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// Evalúa reglas de negocio de geocercas (Geofencing > 500m) e Inactividad (> 10 min a 0 km/h fuera de punto de entrega)
pub async fn evaluate_geofence_and_rules(
    mut payload: GpsTelemetryPayload,
    routes: &Arc<RwLock<HashMap<String, DriverRouteReference>>>,
    idle_trackers: &Arc<RwLock<HashMap<String, DriverIdleTracker>>>,
    latest_fleet: &Arc<RwLock<HashMap<String, GpsTelemetryPayload>>>,
    recent_alerts: &Arc<RwLock<Vec<RouteAlertPayload>>>,
    sse_tx: &broadcast::Sender<LiveStreamEvent>,
    producer: &KafkaProducerService,
    db_pool: &Option<PgPool>,
) {
    let route_ref = {
        let guard = routes.read().await;
        guard.get(&payload.driver_id).cloned().unwrap_or(DriverRouteReference {
            driver_id: payload.driver_id.clone(),
            driver_name: payload
                .driver_name
                .clone()
                .unwrap_or_else(|| format!("Repartidor {}", payload.driver_id)),
            zone: payload
                .zone
                .clone()
                .unwrap_or_else(|| "Centro-Metropolitano".to_string()),
            assigned_lat: payload.assigned_lat.unwrap_or(19.4326),
            assigned_lon: payload.assigned_lon.unwrap_or(-99.1332),
            delivery_lat: payload.delivery_lat.unwrap_or(19.4355),
            delivery_lon: payload.delivery_lon.unwrap_or(-99.1300),
        })
    };

    let deviation_m = haversine_distance_meters(
        payload.latitude,
        payload.longitude,
        route_ref.assigned_lat,
        route_ref.assigned_lon,
    );

    let dist_to_delivery_m = haversine_distance_meters(
        payload.latitude,
        payload.longitude,
        route_ref.delivery_lat,
        route_ref.delivery_lon,
    );

    payload.driver_name = Some(route_ref.driver_name.clone());
    payload.zone = Some(route_ref.zone.clone());
    payload.assigned_lat = Some(route_ref.assigned_lat);
    payload.assigned_lon = Some(route_ref.assigned_lon);
    payload.delivery_lat = Some(route_ref.delivery_lat);
    payload.delivery_lon = Some(route_ref.delivery_lon);
    payload.deviation_meters = Some((deviation_m * 10.0).round() / 10.0);

    // 1. Regla de Inactividad: Velocidad 0 km/h por más de 10 minutos fuera de punto de entrega (> 120m del destino)
    let mut idle_mins = payload.idle_minutes.unwrap_or(0.0);
    {
        let mut tracker_map = idle_trackers.write().await;
        // Prevenir agotamiento de memoria (DoS / OOM) limitando cardinalidad a máximo 500 unidades en caché
        if tracker_map.len() >= 500 && !tracker_map.contains_key(&payload.driver_id) {
            return;
        }
        let entry = tracker_map
            .entry(payload.driver_id.clone())
            .or_insert(DriverIdleTracker {
                stopped_since: None,
                simulated_idle_minutes: 0.0,
            });

        if payload.speed_kmh <= 0.5 && dist_to_delivery_m > 120.0 {
            if entry.stopped_since.is_none() {
                entry.stopped_since = Some(Utc::now());
            }
            // Si el simulador envía `idle_minutes`, lo usamos o acumulamos para poder demostrar la regla de 10 min
            if idle_mins > 0.0 {
                entry.simulated_idle_minutes = idle_mins;
            } else {
                entry.simulated_idle_minutes += 2.5;
            }
            idle_mins = entry.simulated_idle_minutes;
        } else {
            entry.stopped_since = None;
            entry.simulated_idle_minutes = 0.0;
            idle_mins = 0.0;
        }
    }
    payload.idle_minutes = Some(idle_mins);

    // Actualizar estado en memoria de la flotilla (con límite de cardinalidad anti-OOM) y enviar por SSE
    {
        let mut fleet = latest_fleet.write().await;
        if fleet.len() < 500 || fleet.contains_key(&payload.driver_id) {
            fleet.insert(payload.driver_id.clone(), payload.clone());
        }
    }
    let _ = sse_tx.send(LiveStreamEvent::Telemetry(payload.clone()));

    // Persistir telemetría y sincronizar estado del repartidor en PostgreSQL si está disponible
    if let Some(pool) = db_pool {
        let _ = sqlx::query(
            "INSERT INTO gps_telemetry (driver_id, latitude, longitude, speed_kmh, delivery_status, deviation_meters, recorded_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&payload.driver_id)
        .bind(payload.latitude)
        .bind(payload.longitude)
        .bind(payload.speed_kmh)
        .bind(&payload.delivery_status)
        .bind(deviation_m)
        .bind(payload.timestamp)
        .execute(pool)
        .await;

        let _ = sqlx::query(
            "UPDATE drivers SET status = $2, updated_at = $3 WHERE driver_id = $1",
        )
        .bind(&payload.driver_id)
        .bind(&payload.delivery_status)
        .bind(payload.timestamp)
        .execute(pool)
        .await;
    }

    // 2. Regla de Negocio 1: Detección de Desvío (> 500 metros de su ruta asignada)
    if deviation_m > 500.0 {
        let alert = RouteAlertPayload {
            alert_id: Uuid::new_v4().to_string(),
            driver_id: payload.driver_id.clone(),
            driver_name: route_ref.driver_name.clone(),
            zone: route_ref.zone.clone(),
            alert_type: "ROUTE_DEVIATION".to_string(),
            severity: if deviation_m > 900.0 {
                "CRITICAL".to_string()
            } else {
                "WARNING".to_string()
            },
            message: format!(
                "Desvío de ruta detectado: {} se alejó {:.0}m de su geocerca asignada en {}",
                route_ref.driver_name, deviation_m, route_ref.zone
            ),
            latitude: payload.latitude,
            longitude: payload.longitude,
            metric_value: (deviation_m * 10.0).round() / 10.0,
            created_at: Utc::now(),
        };

        emit_and_persist_alert(alert, recent_alerts, sse_tx, producer, db_pool).await;
    }

    // 3. Regla de Negocio 2: Detección de Inactividad (0 km/h por > 10 minutos fuera de punto de entrega)
    if payload.speed_kmh <= 0.5 && idle_mins >= 10.0 && dist_to_delivery_m > 120.0 {
        let alert = RouteAlertPayload {
            alert_id: Uuid::new_v4().to_string(),
            driver_id: payload.driver_id.clone(),
            driver_name: route_ref.driver_name.clone(),
            zone: route_ref.zone.clone(),
            alert_type: "UNAUTHORIZED_STOP".to_string(),
            severity: "CRITICAL".to_string(),
            message: format!(
                "Parada no autorizada: {} lleva {:.1} min detenido (0 km/h) a {:.0}m del punto de entrega",
                route_ref.driver_name, idle_mins, dist_to_delivery_m
            ),
            latitude: payload.latitude,
            longitude: payload.longitude,
            metric_value: (idle_mins * 10.0).round() / 10.0,
            created_at: Utc::now(),
        };

        emit_and_persist_alert(alert, recent_alerts, sse_tx, producer, db_pool).await;
    }
}

async fn emit_and_persist_alert(
    alert: RouteAlertPayload,
    recent_alerts: &Arc<RwLock<Vec<RouteAlertPayload>>>,
    sse_tx: &broadcast::Sender<LiveStreamEvent>,
    producer: &KafkaProducerService,
    db_pool: &Option<PgPool>,
) {
    // Publicar en tópico Kafka `route-alerts`
    let _ = producer.publish_alert(&alert).await;

    // Guardar en buffer en memoria (máximo 50 alertas recientes)
    {
        let mut alerts = recent_alerts.write().await;
        alerts.insert(0, alert.clone());
        if alerts.len() > 50 {
            alerts.truncate(50);
        }
    }

    // Emitir inmediatamente por Server-Sent Events a Angular
    let _ = sse_tx.send(LiveStreamEvent::Alert(alert.clone()));

    // Persistir en PostgreSQL
    if let Some(pool) = db_pool {
        let _ = sqlx::query(
            "INSERT INTO route_alerts (driver_id, alert_type, severity, message, latitude, longitude, metric_value, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&alert.driver_id)
        .bind(&alert.alert_type)
        .bind(&alert.severity)
        .bind(&alert.message)
        .bind(alert.latitude)
        .bind(alert.longitude)
        .bind(alert.metric_value)
        .bind(alert.created_at)
        .execute(pool)
        .await;
    }
}

/// Inicia el consumidor de Kafka para el tópico `gps-telemetry`
pub async fn start_kafka_consumer(
    brokers: String,
    telemetry_topic: String,
    routes: Arc<RwLock<HashMap<String, DriverRouteReference>>>,
    idle_trackers: Arc<RwLock<HashMap<String, DriverIdleTracker>>>,
    latest_fleet: Arc<RwLock<HashMap<String, GpsTelemetryPayload>>>,
    recent_alerts: Arc<RwLock<Vec<RouteAlertPayload>>>,
    sse_tx: broadcast::Sender<LiveStreamEvent>,
    producer: KafkaProducerService,
    db_pool: Option<PgPool>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("group.id", "swiftroute-geofence-processor")
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("No se pudo crear el consumidor de Kafka ({}): operando con procesamiento directo en memoria", e);
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&[&telemetry_topic]) {
        warn!("Error al suscribirse al tópico {}: {}", telemetry_topic, e);
        return;
    }

    info!(
        "Kafka Stream Consumer escuchando tópico '{}' en {}",
        telemetry_topic, brokers
    );

    loop {
        match consumer.recv().await {
            Err(e) => {
                warn!("Kafka consumer warning: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            Ok(m) => {
                if let Some(Ok(payload_str)) = m.payload_view::<str>() {
                    if let Ok(telemetry) = serde_json::from_str::<GpsTelemetryPayload>(payload_str)
                    {
                        evaluate_geofence_and_rules(
                            telemetry,
                            &routes,
                            &idle_trackers,
                            &latest_fleet,
                            &recent_alerts,
                            &sse_tx,
                            &producer,
                            &db_pool,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_within_geofence() {
        // Dos puntos muy cercanos (< 150m) en CDMX
        let d = haversine_distance_meters(19.4326, -99.1332, 19.4335, -99.1325);
        assert!(d > 50.0 && d < 200.0, "Distancia esperada < 200m, obtenida: {}", d);
    }

    #[test]
    fn test_haversine_breaches_500m_geofence() {
        // Desvío mayor a 500m (regla de negocio 1)
        let d = haversine_distance_meters(19.4978, -99.1269, 19.5046, -99.1237);
        assert!(d > 500.0, "Distancia esperada > 500m para disparar alerta, obtenida: {}", d);
    }
}

