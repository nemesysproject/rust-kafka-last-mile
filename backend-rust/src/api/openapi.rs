use axum::response::Html;
use axum::Json;

/// Especificacion OpenAPI 3.0.3 completa del Core API y Stream Processor en Rust
pub async fn openapi_json_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "openapi": "3.0.3",
        "info": {
            "title": "SwiftRoute Last-Mile · Core API & Stream Processor (Rust)",
            "version": "1.0.0",
            "description": "API REST y Server-Sent Events (SSE) para ingesta de telemetria GPS en tiempo real, publicacion en Apache Kafka (`gps-telemetry` y `route-alerts`), evaluacion de geocercas (>500m) e inactividad (>10 min a 0 km/h), simulador de flotilla y consulta de KPIs diarios consolidados por Apache Airflow en PostgreSQL."
        },
        "servers": [
            {
                "url": "http://localhost:8080",
                "description": "Servidor Local / Contenedor Docker Rust (:8080)"
            }
        ],
        "tags": [
            { "name": "Health", "description": "Estado operativo del servidor, simulador y conexion PostgreSQL" },
            { "name": "Telemetry & Fleet", "description": "Ingesta de coordenadas GPS, estado en vivo de la flotilla y alertas de geocercas" },
            { "name": "Simulator", "description": "Generador de trafico GPS y disparo bajo demanda de desvio (>500m) o parada (>10m)" },
            { "name": "Batch KPIs", "description": "Consulta de metricas diarias y liquidaciones consolidadas en PostgreSQL por Airflow" },
            { "name": "Real-Time SSE", "description": "Flujo continuo Server-Sent Events hacia Angular 21 Signals" }
        ],
        "paths": {
            "/api/health": {
                "get": {
                    "tags": ["Health"],
                    "summary": "Verificar estado de salud del servicio Rust",
                    "operationId": "getHealth",
                    "responses": {
                        "200": {
                            "description": "Servicio activo",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/HealthResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/telemetry": {
                "post": {
                    "tags": ["Telemetry & Fleet"],
                    "summary": "Ingestar paquete de telemetria GPS de un repartidor",
                    "description": "Publica el evento en el topico Kafka `gps-telemetry` particionado por `driver_id` y evalua reglas de geocerca (>500m) e inactividad (>10m a 0 km/h).",
                    "operationId": "ingestTelemetry",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/IngestTelemetryRequest" },
                                "example": {
                                    "driver_id": "DRV-001",
                                    "latitude": 19.5046,
                                    "longitude": -99.1237,
                                    "speed_kmh": 54.2,
                                    "delivery_status": "IN_TRANSIT",
                                    "idle_minutes": 0.0
                                }
                            }
                        }
                    },
                    "responses": {
                        "202": {
                            "description": "Coordenada GPS aceptada y procesada",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/IngestTelemetryResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/fleet/state": {
                "get": {
                    "tags": ["Telemetry & Fleet"],
                    "summary": "Obtener snapshot actual de toda la flotilla y alertas activas",
                    "operationId": "getFleetState",
                    "responses": {
                        "200": {
                            "description": "Estado actual de los vehiculos y alertas recientes",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/FleetStatusResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/alerts": {
                "get": {
                    "tags": ["Telemetry & Fleet"],
                    "summary": "Listar alertas recientes de desvio (>500m) o parada no autorizada (>10m)",
                    "operationId": "getAlerts",
                    "parameters": [
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "description": "Cantidad maxima de alertas a retornar (por defecto 25)",
                            "schema": { "type": "integer", "default": 25 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Listado de alertas del topico `route-alerts`",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/RouteAlertPayload" }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/simulator/trigger": {
                "post": {
                    "tags": ["Simulator"],
                    "summary": "Disparar rafaga del simulador o forzar anomalias de negocio",
                    "description": "Permite emitir pulsos GPS normales (`normal`), forzar un desvio de ruta mayor a 500 metros (`deviation`), forzar una parada no autorizada mayor a 10 minutos a 0 km/h (`idle`), o pausar/reanudar el simulador automatico (`toggle_auto`).",
                    "operationId": "triggerSimulator",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/SimulationTriggerRequest" },
                                "examples": {
                                    "deviation": {
                                        "summary": "Forzar Desvio de Geocerca (>500m)",
                                        "value": { "mode": "deviation", "burst_count": 1 }
                                    },
                                    "idle": {
                                        "summary": "Forzar Parada No Autorizada (>10 min a 0 km/h)",
                                        "value": { "mode": "idle", "burst_count": 1 }
                                    },
                                    "normal": {
                                        "summary": "Pulso GPS Normal de toda la flotilla",
                                        "value": { "mode": "normal", "burst_count": 1 }
                                    },
                                    "toggle": {
                                        "summary": "Pausar / Reanudar Simulador Automatico",
                                        "value": { "mode": "toggle_auto" }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Resultado de la ejecucion del simulador",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SimulationTriggerResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/kpis": {
                "get": {
                    "tags": ["Batch KPIs"],
                    "summary": "Obtener KPIs diarios y liquidacion por zona desde PostgreSQL",
                    "description": "Retorna las metricas consolidadas en la tabla `daily_zone_kpis` por el DAG `daily_fleet_kpi` de Apache Airflow.",
                    "operationId": "getDailyKpis",
                    "responses": {
                        "200": {
                            "description": "Reporte diario de rendimiento por zona",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/DailyZoneKpiDto" }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/sse/telemetry": {
                "get": {
                    "tags": ["Real-Time SSE"],
                    "summary": "Stream Server-Sent Events (SSE) en tiempo real",
                    "description": "Canal unidireccional (`text/event-stream`) consumido con `EventSource` desde Angular 21. Emite eventos JSON con `event_type: 'telemetry' | 'alert'`.",
                    "operationId": "streamTelemetrySse",
                    "responses": {
                        "200": {
                            "description": "Stream de eventos SSE (`text/event-stream`)"
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "HealthResponse": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "example": "UP" },
                        "service": { "type": "string", "example": "swiftroute-rust-core-api" },
                        "simulator_running": { "type": "boolean", "example": true },
                        "postgres_connected": { "type": "boolean", "example": true },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "IngestTelemetryRequest": {
                    "type": "object",
                    "required": ["driver_id", "latitude", "longitude", "speed_kmh"],
                    "properties": {
                        "driver_id": { "type": "string", "example": "DRV-001" },
                        "latitude": { "type": "number", "format": "double", "example": 19.5046 },
                        "longitude": { "type": "number", "format": "double", "example": -99.1237 },
                        "speed_kmh": { "type": "number", "format": "double", "example": 54.2 },
                        "delivery_status": { "type": "string", "example": "IN_TRANSIT" },
                        "idle_minutes": { "type": "number", "format": "double", "example": 0.0 }
                    }
                },
                "IngestTelemetryResponse": {
                    "type": "object",
                    "properties": {
                        "accepted": { "type": "boolean", "example": true },
                        "driver_id": { "type": "string", "example": "DRV-001" },
                        "kafka_broker_published": { "type": "boolean", "example": true },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "GpsTelemetryPayload": {
                    "type": "object",
                    "properties": {
                        "driver_id": { "type": "string", "example": "DRV-001" },
                        "driver_name": { "type": "string", "example": "Carlos Mendoza" },
                        "zone": { "type": "string", "example": "Norte-Industrial" },
                        "latitude": { "type": "number", "format": "double", "example": 19.4985 },
                        "longitude": { "type": "number", "format": "double", "example": -99.1261 },
                        "speed_kmh": { "type": "number", "format": "double", "example": 44.2 },
                        "delivery_status": { "type": "string", "example": "IN_TRANSIT" },
                        "assigned_lat": { "type": "number", "format": "double", "example": 19.4978 },
                        "assigned_lon": { "type": "number", "format": "double", "example": -99.1269 },
                        "delivery_lat": { "type": "number", "format": "double", "example": 19.5010 },
                        "delivery_lon": { "type": "number", "format": "double", "example": -99.1240 },
                        "deviation_meters": { "type": "number", "format": "double", "example": 112.4 },
                        "idle_minutes": { "type": "number", "format": "double", "example": 0.0 },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "RouteAlertPayload": {
                    "type": "object",
                    "properties": {
                        "alert_id": { "type": "string", "format": "uuid" },
                        "driver_id": { "type": "string", "example": "DRV-001" },
                        "driver_name": { "type": "string", "example": "Carlos Mendoza" },
                        "zone": { "type": "string", "example": "Norte-Industrial" },
                        "alert_type": { "type": "string", "enum": ["ROUTE_DEVIATION", "UNAUTHORIZED_STOP"] },
                        "severity": { "type": "string", "enum": ["WARNING", "CRITICAL"] },
                        "message": { "type": "string", "example": "Desvio de ruta detectado: Carlos Mendoza se alejo 825m de su geocerca asignada" },
                        "latitude": { "type": "number", "format": "double", "example": 19.5046 },
                        "longitude": { "type": "number", "format": "double", "example": -99.1237 },
                        "metric_value": { "type": "number", "format": "double", "example": 824.6 },
                        "created_at": { "type": "string", "format": "date-time" }
                    }
                },
                "FleetStatusResponse": {
                    "type": "object",
                    "properties": {
                        "simulator_active": { "type": "boolean", "example": true },
                        "active_drivers": { "type": "integer", "example": 6 },
                        "total_recent_alerts": { "type": "integer", "example": 2 },
                        "vehicles": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/GpsTelemetryPayload" }
                        },
                        "alerts": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/RouteAlertPayload" }
                        }
                    }
                },
                "SimulationTriggerRequest": {
                    "type": "object",
                    "properties": {
                        "mode": {
                            "type": "string",
                            "enum": ["normal", "deviation", "idle", "toggle_auto"],
                            "example": "deviation"
                        },
                        "burst_count": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 20,
                            "example": 1
                        }
                    }
                },
                "SimulationTriggerResponse": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "example": "executed" },
                        "mode": { "type": "string", "example": "deviation" },
                        "bursts": { "type": "integer", "example": 1 },
                        "events_emitted": { "type": "integer", "example": 6 },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "DailyZoneKpiDto": {
                    "type": "object",
                    "properties": {
                        "report_date": { "type": "string", "format": "date", "example": "2026-11-18" },
                        "zone": { "type": "string", "example": "Norte-Industrial" },
                        "total_deliveries": { "type": "integer", "example": 24 },
                        "successful_deliveries": { "type": "integer", "example": 23 },
                        "on_time_rate": { "type": "number", "format": "double", "example": 95.8 },
                        "total_alerts": { "type": "integer", "example": 1 },
                        "driver_payout_usd": { "type": "number", "format": "double", "example": 145.5 },
                        "computed_at": { "type": "string", "format": "date-time" }
                    }
                }
            }
        }
    }))
}

/// Interfaz interactiva Swagger UI servida en `/swagger-ui` y `/api/docs`
pub async fn swagger_ui_handler() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SwiftRoute Rust API · Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css" />
  <style>
    body { margin: 0; background: #fafafa; }
    .topbar-custom {
      background: #0f172a;
      color: #f8fafc;
      padding: 12px 24px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 2px solid #38bdf8;
    }
    .topbar-custom strong { color: #38bdf8; }
    .topbar-custom a {
      color: #7dd3fc;
      text-decoration: none;
      font-size: 0.85rem;
    }
  </style>
</head>
<body>
  <div class="topbar-custom">
    <div><strong>SwiftRoute Last-Mile</strong> · Rust Axum Core API &amp; Stream Processor (OpenAPI 3.0)</div>
    <a href="/api/openapi.json" target="_blank">Ver /api/openapi.json</a>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: '/api/openapi.json',
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIBundle.SwaggerUIStandalonePreset
        ],
        layout: "BaseLayout"
      });
    };
  </script>
</body>
</html>"#,
    )
}
