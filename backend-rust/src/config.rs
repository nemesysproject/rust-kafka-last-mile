use std::env;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub database_url: String,
    pub host: String,
    pub port: u16,
    pub telemetry_topic: String,
    pub alerts_topic: String,
    pub allowed_origins: Vec<String>,
    pub api_key: Option<String>,
    pub max_tracked_drivers: usize,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        let allowed_origins = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:4200,http://localhost:8080".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let api_key = env::var("SWIFTROUTE_API_KEY")
            .ok()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());

        let max_tracked_drivers = env::var("MAX_TRACKED_DRIVERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);

        Self {
            kafka_brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://localhost:5432/swiftroute".to_string()),
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            telemetry_topic: env::var("KAFKA_TELEMETRY_TOPIC")
                .unwrap_or_else(|_| "gps-telemetry".to_string()),
            alerts_topic: env::var("KAFKA_ALERTS_TOPIC")
                .unwrap_or_else(|_| "route-alerts".to_string()),
            allowed_origins,
            api_key,
            max_tracked_drivers,
        }
    }
}
