mod api;
mod config;
mod kafka;
mod simulator;

use crate::api::handlers::{
    fetch_alerts_from_postgres, fetch_fleet_from_postgres, get_alerts_handler,
    get_daily_kpis_handler, get_deliveries_handler, get_fleet_state_handler,
    get_route_optimizations_handler, health_handler, ingest_telemetry_handler,
    trigger_simulator_handler, AppState,
};
use crate::api::openapi::{openapi_json_handler, swagger_ui_handler};
use crate::api::sse::telemetry_sse_handler;
use crate::config::AppConfig;
use crate::kafka::consumer::{start_kafka_consumer, DriverRouteReference};
use crate::kafka::producer::KafkaProducerService;
use crate::simulator::fleet_gen::{default_fleet_routes, start_background_fleet_simulator};
use axum::{
    extract::DefaultBodyLimit,
    http::{HeaderName, HeaderValue, Method},
    routing::{get, post},
    Router,
};
use sqlx::{postgres::PgPoolOptions, Row};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = AppConfig::from_env();
    info!("Iniciando SwiftRoute Rust Core API & Stream Processor...");

    // Conexión a PostgreSQL
    let db_pool = match PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(4))
        .connect(&cfg.database_url)
        .await
    {
        Ok(pool) => {
            info!("Conexión establecida con PostgreSQL");
            Some(pool)
        }
        Err(e) => {
            warn!(
                "PostgreSQL no disponible ({}): ejecutando con estado en memoria",
                e
            );
            None
        }
    };

    // Cargar rutas, última posición de flotilla y alertas recientes desde PostgreSQL al arrancar
    let mut initial_routes = HashMap::new();
    let mut initial_fleet = HashMap::new();
    let mut initial_alerts = Vec::new();

    if let Some(ref pool) = db_pool {
        if let Ok(rows) = sqlx::query(
            "SELECT driver_id, driver_name, zone, assigned_lat, assigned_lon, delivery_lat, delivery_lon FROM drivers",
        )
        .fetch_all(pool)
        .await
        {
            for r in rows {
                let driver_id: String = r.get("driver_id");
                initial_routes.insert(
                    driver_id.clone(),
                    DriverRouteReference {
                        driver_id,
                        driver_name: r.get("driver_name"),
                        zone: r.get("zone"),
                        assigned_lat: r.get("assigned_lat"),
                        assigned_lon: r.get("assigned_lon"),
                        delivery_lat: r.get("delivery_lat"),
                        delivery_lon: r.get("delivery_lon"),
                    },
                );
            }
            info!("Cargados {} repartidores desde la tabla 'drivers' en PostgreSQL", initial_routes.len());
        }

        if let Ok(fleet_list) = fetch_fleet_from_postgres(pool).await {
            for v in fleet_list {
                initial_fleet.insert(v.driver_id.clone(), v);
            }
        }

        if let Ok(alerts_list) = fetch_alerts_from_postgres(pool, 50).await {
            initial_alerts = alerts_list;
        }
    }

    if initial_routes.is_empty() {
        initial_routes = default_fleet_routes();
    }

    let producer = KafkaProducerService::new(
        &cfg.kafka_brokers,
        &cfg.telemetry_topic,
        &cfg.alerts_topic,
    );

    let (sse_tx, _) = broadcast::channel(512);
    let routes = Arc::new(RwLock::new(initial_routes));
    let idle_trackers = Arc::new(RwLock::new(HashMap::new()));
    let latest_fleet = Arc::new(RwLock::new(initial_fleet));
    let recent_alerts = Arc::new(RwLock::new(initial_alerts));
    let simulator_running = Arc::new(AtomicBool::new(true));

    // Lanzar consumidor de Kafka en background
    tokio::spawn(start_kafka_consumer(
        cfg.kafka_brokers.clone(),
        cfg.telemetry_topic.clone(),
        routes.clone(),
        idle_trackers.clone(),
        latest_fleet.clone(),
        recent_alerts.clone(),
        sse_tx.clone(),
        producer.clone(),
        db_pool.clone(),
    ));

    // Lanzar simulador de flotilla continuo en background
    tokio::spawn(start_background_fleet_simulator(
        simulator_running.clone(),
        routes.clone(),
        idle_trackers.clone(),
        latest_fleet.clone(),
        recent_alerts.clone(),
        sse_tx.clone(),
        producer.clone(),
        db_pool.clone(),
    ));

    let state = AppState {
        producer,
        sse_tx,
        routes,
        idle_trackers,
        latest_fleet,
        recent_alerts,
        simulator_running,
        db_pool,
        api_key: cfg.api_key.clone(),
        max_tracked_drivers: cfg.max_tracked_drivers,
    };

    // Configurar CORS restringido a origenes permitidos (ALLOWED_ORIGINS) y metodos explicitos
    let parsed_origins: Vec<HeaderValue> = cfg
        .allowed_origins
        .iter()
        .filter_map(|o| o.parse::<HeaderValue>().ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed_origins))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            HeaderName::from_static("content-type"),
            HeaderName::from_static("authorization"),
            HeaderName::from_static("x-api-key"),
        ]);

    let app = Router::new()
        .route("/api/health", get(health_handler))
        .route("/api/telemetry", post(ingest_telemetry_handler))
        .route("/api/fleet/state", get(get_fleet_state_handler))
        .route("/api/alerts", get(get_alerts_handler))
        .route("/api/kpis", get(get_daily_kpis_handler))
        .route("/api/deliveries", get(get_deliveries_handler))
        .route("/api/optimizations", get(get_route_optimizations_handler))
        .route("/api/simulator/trigger", post(trigger_simulator_handler))
        .route("/api/sse/telemetry", get(telemetry_sse_handler))
        .route("/api/openapi.json", get(openapi_json_handler))
        .route("/api/docs", get(swagger_ui_handler))
        .route("/swagger-ui", get(swagger_ui_handler))
        .layer(DefaultBodyLimit::max(64 * 1024)) // Limite de 64KB en payload HTTP
        .layer(cors)
        .with_state(state);

    let bind_addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .expect("No se pudo abrir el puerto HTTP");

    info!("Servidor HTTP/SSE escuchando en http://{}", bind_addr);
    axum::serve(listener, app).await.unwrap();
}
