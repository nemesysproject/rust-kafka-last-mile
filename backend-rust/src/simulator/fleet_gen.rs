use crate::kafka::consumer::{
    evaluate_geofence_and_rules, DriverIdleTracker, DriverRouteReference, LiveStreamEvent,
};
use crate::kafka::producer::{GpsTelemetryPayload, KafkaProducerService, RouteAlertPayload};
use chrono::Utc;
use rand::Rng;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::info;

pub fn default_fleet_routes() -> HashMap<String, DriverRouteReference> {
    let list = vec![
        DriverRouteReference {
            driver_id: "DRV-001".to_string(),
            driver_name: "Carlos Mendoza".to_string(),
            zone: "Norte-Industrial".to_string(),
            assigned_lat: 19.4978,
            assigned_lon: -99.1269,
            delivery_lat: 19.5010,
            delivery_lon: -99.1240,
        },
        DriverRouteReference {
            driver_id: "DRV-002".to_string(),
            driver_name: "Laura Villarreal".to_string(),
            zone: "Centro-Historico".to_string(),
            assigned_lat: 19.4326,
            assigned_lon: -99.1332,
            delivery_lat: 19.4350,
            delivery_lon: -99.1300,
        },
        DriverRouteReference {
            driver_id: "DRV-003".to_string(),
            driver_name: "Miguel Ángel Soto".to_string(),
            zone: "Sur-Coyoacan".to_string(),
            assigned_lat: 19.3467,
            assigned_lon: -99.1617,
            delivery_lat: 19.3500,
            delivery_lon: -99.1600,
        },
        DriverRouteReference {
            driver_id: "DRV-004".to_string(),
            driver_name: "Sofía Hernández".to_string(),
            zone: "Poniente-SantaFe".to_string(),
            assigned_lat: 19.3663,
            assigned_lon: -99.2612,
            delivery_lat: 19.3690,
            delivery_lon: -99.2580,
        },
        DriverRouteReference {
            driver_id: "DRV-005".to_string(),
            driver_name: "Ricardo Téllez".to_string(),
            zone: "Oriente-Aeropuerto".to_string(),
            assigned_lat: 19.4361,
            assigned_lon: -99.0719,
            delivery_lat: 19.4400,
            delivery_lon: -99.0690,
        },
        DriverRouteReference {
            driver_id: "DRV-006".to_string(),
            driver_name: "Elena Gómez".to_string(),
            zone: "Centro-Roma".to_string(),
            assigned_lat: 19.4194,
            assigned_lon: -99.1626,
            delivery_lat: 19.4220,
            delivery_lon: -99.1600,
        },
    ];

    list.into_iter().map(|d| (d.driver_id.clone(), d)).collect()
}

/// Genera un ciclo o ráfaga de simulación para todos los vehículos registrados
/// Incluye escenarios normales, desvíos (> 500m) y paradas no autorizadas (> 10 min a 0 km/h)
pub async fn simulate_fleet_step(
    anomaly_mode: Option<&str>,
    routes: &Arc<RwLock<HashMap<String, DriverRouteReference>>>,
    idle_trackers: &Arc<RwLock<HashMap<String, DriverIdleTracker>>>,
    latest_fleet: &Arc<RwLock<HashMap<String, GpsTelemetryPayload>>>,
    recent_alerts: &Arc<RwLock<Vec<RouteAlertPayload>>>,
    sse_tx: &broadcast::Sender<LiveStreamEvent>,
    producer: &KafkaProducerService,
    db_pool: &Option<PgPool>,
) -> Vec<GpsTelemetryPayload> {
    let route_list: Vec<DriverRouteReference> = {
        let guard = routes.read().await;
        let mut v: Vec<_> = guard.values().cloned().collect();
        v.sort_by(|a, b| a.driver_id.cmp(&b.driver_id));
        v
    };

    let mut generated = Vec::with_capacity(route_list.len());

    for (idx, driver) in route_list.iter().enumerate() {
        let (lat_offset, lon_offset, speed_kmh, status, idle_minutes): (f64, f64, f64, &str, f64) = {
            let mut rng = rand::thread_rng();
            match anomaly_mode {
                Some("deviation") if idx == 0 => {
                    // Forzar desvío de ~780 metros en DRV-001
                    (0.0068, 0.0032, rng.gen_range(42.0..68.0), "OFF_ROUTE", 0.0)
                }
                Some("idle") if idx == 2 => {
                    // Forzar parada no autorizada (> 10 min a 0 km/h lejos del destino) en DRV-003
                    (0.0021, -0.0019, 0.0, "IDLE_STOPPED", 12.5)
                }
                _ => {
                    // 12% probabilidad de desvío espontáneo o parada para mantener vivo el demo
                    let roll: f64 = rng.gen();
                    if roll < 0.07 {
                        (
                            rng.gen_range(0.0052..0.0085),
                            rng.gen_range(0.0020..0.0050),
                            rng.gen_range(38.0..62.0),
                            "OFF_ROUTE",
                            0.0,
                        )
                    } else if roll < 0.12 {
                        (0.0022, -0.0018, 0.0, "IDLE_STOPPED", 11.2)
                    } else {
                        (
                            rng.gen_range(-0.0018..0.0018),
                            rng.gen_range(-0.0018..0.0018),
                            rng.gen_range(22.0..54.0),
                            "IN_TRANSIT",
                            0.0,
                        )
                    }
                }
            }
        };

        let payload = GpsTelemetryPayload {
            driver_id: driver.driver_id.clone(),
            driver_name: Some(driver.driver_name.clone()),
            zone: Some(driver.zone.clone()),
            latitude: ((driver.assigned_lat + lat_offset) * 100000.0).round() / 100000.0,
            longitude: ((driver.assigned_lon + lon_offset) * 100000.0).round() / 100000.0,
            speed_kmh: (speed_kmh * 10.0).round() / 10.0,
            delivery_status: status.to_string(),
            assigned_lat: Some(driver.assigned_lat),
            assigned_lon: Some(driver.assigned_lon),
            delivery_lat: Some(driver.delivery_lat),
            delivery_lon: Some(driver.delivery_lon),
            deviation_meters: None,
            idle_minutes: Some(idle_minutes),
            timestamp: Utc::now(),
        };

        // Intentar publicar en Kafka; si Kafka no está activo en local sin docker, evaluar directamente
        if producer.publish_telemetry(&payload).await.is_err() {
            evaluate_geofence_and_rules(
                payload.clone(),
                routes,
                idle_trackers,
                latest_fleet,
                recent_alerts,
                sse_tx,
                producer,
                db_pool,
            )
            .await;
        }

        generated.push(payload);
    }

    generated
}

/// Tarea background que simula la flotilla continuamente cada 3 segundos
pub async fn start_background_fleet_simulator(
    running: Arc<AtomicBool>,
    routes: Arc<RwLock<HashMap<String, DriverRouteReference>>>,
    idle_trackers: Arc<RwLock<HashMap<String, DriverIdleTracker>>>,
    latest_fleet: Arc<RwLock<HashMap<String, GpsTelemetryPayload>>>,
    recent_alerts: Arc<RwLock<Vec<RouteAlertPayload>>>,
    sse_tx: broadcast::Sender<LiveStreamEvent>,
    producer: KafkaProducerService,
    db_pool: Option<PgPool>,
) {
    info!("Simulador automático de flotilla GPS iniciado (intervalo: 3s)");
    loop {
        if running.load(Ordering::Relaxed) {
            let _ = simulate_fleet_step(
                None,
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
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
