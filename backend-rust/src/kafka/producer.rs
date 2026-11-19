use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpsTelemetryPayload {
    pub driver_id: String,
    pub driver_name: Option<String>,
    pub zone: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub speed_kmh: f64,
    pub delivery_status: String,
    pub assigned_lat: Option<f64>,
    pub assigned_lon: Option<f64>,
    pub delivery_lat: Option<f64>,
    pub delivery_lon: Option<f64>,
    pub deviation_meters: Option<f64>,
    pub idle_minutes: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteAlertPayload {
    pub alert_id: String,
    pub driver_id: String,
    pub driver_name: String,
    pub zone: String,
    pub alert_type: String, // "ROUTE_DEVIATION" | "UNAUTHORIZED_STOP"
    pub severity: String,   // "WARNING" | "CRITICAL"
    pub message: String,
    pub latitude: f64,
    pub longitude: f64,
    pub metric_value: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct KafkaProducerService {
    producer: Option<FutureProducer>,
    telemetry_topic: String,
    alerts_topic: String,
}

impl KafkaProducerService {
    pub fn new(brokers: &str, telemetry_topic: &str, alerts_topic: &str) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
            .ok();

        if producer.is_some() {
            info!("Kafka Producer initialized for brokers: {}", brokers);
        } else {
            error!("Failed to initialize Kafka Producer; operating in fallback broadcast mode");
        }

        Self {
            producer,
            telemetry_topic: telemetry_topic.to_string(),
            alerts_topic: alerts_topic.to_string(),
        }
    }

    /// Publica telemetría GPS en el tópico `gps-telemetry` particionado por `driver_id`
    pub async fn publish_telemetry(&self, payload: &GpsTelemetryPayload) -> Result<(), String> {
        let Some(ref producer) = self.producer else {
            return Err("Kafka producer not available".to_string());
        };

        let json = serde_json::to_string(payload).map_err(|e| e.to_string())?;
        let record = FutureRecord::to(&self.telemetry_topic)
            .key(&payload.driver_id)
            .payload(&json);

        producer
            .send(record, Duration::from_secs(2))
            .await
            .map(|_| ())
            .map_err(|(err, _)| format!("Kafka telemetry publish error: {}", err))
    }

    /// Publica una alerta en el tópico `route-alerts` particionada por `driver_id`
    pub async fn publish_alert(&self, alert: &RouteAlertPayload) -> Result<(), String> {
        let Some(ref producer) = self.producer else {
            return Err("Kafka producer not available".to_string());
        };

        let json = serde_json::to_string(alert).map_err(|e| e.to_string())?;
        let record = FutureRecord::to(&self.alerts_topic)
            .key(&alert.driver_id)
            .payload(&json);

        producer
            .send(record, Duration::from_secs(2))
            .await
            .map(|_| ())
            .map_err(|(err, _)| format!("Kafka alert publish error: {}", err))
    }
}
