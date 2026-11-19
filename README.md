# SwiftRoute Last-Mile · Telemetría GPS en Tiempo Real y Orquestación Batch

## 1. Documento de Especificación Funcional (PRD)

### 1.1. Objetivo del Sistema

Procesar telemetría GPS de una flotilla de repartidores en tiempo real para detectar retrasos o desvíos al instante, mientras se automatizan los cierres operativos diarios y la optimización de rutas mediante procesos por lotes.

### 1.2. Actores y Componentes

- **Simulador de Flotilla (Rust CLI/Task)**: Genera coordenadas GPS, velocidad y estado de entrega de cientos de vehículos simultáneamente.
- **Core API & Stream Processor (Rust)**: Recibe la telemetría, la publica en Kafka, evalúa reglas de geocercas (geofencing) y expone métricas en vivo hacia el frontend.
- **Message Broker (Apache Kafka)**: Desacopla la ingesta masiva de datos mediante tópicos particionados por `driver_id` (`gps-telemetry` y `route-alerts`).
- **Orquestador Batch (Apache Airflow)**: Ejecuta flujos programados para calcular métricas de eficiencia diaria, liquidación de pagos a repartidores y limpieza de datos históricos.
- **Dashboard Operativo (Angular 21)**: Interfaz reactiva que consume datos en tiempo real vía Server-Sent Events (SSE) con Signals y consulta/dispara el estado de los DAGs de Airflow.

### 1.3. Reglas de Negocio Clave

- **Detección de Desvío**: Si un repartidor se aleja más de **500 metros** de su ruta asignada (calculado vía fórmula Haversine), Rust emite una alerta al tópico `route-alerts` y al stream SSE.
- **Detección de Inactividad**: Si un vehículo en ruta reporta velocidad **0 km/h por más de 10 minutos** fuera de un punto de entrega, se dispara una alerta crítica de parada no autorizada.
- **Cierre Diario (Batch)**: Cada 24 horas (o bajo demanda desde el dashboard), Airflow consolida las entregas exitosas en PostgreSQL y genera un reporte de rendimiento y liquidación por zona.

---

## 2. Organización de Directorios (Monorepo)

Esta estructura facilita el desarrollo local y mantiene separados los contextos de cada tecnología:

```text
rust-kafka-last-mile/
├── docker-compose.yml               # Orquestación de toda la infraestructura
├── .env                             # Variables de entorno globales
├── build-test-deploy.ps1            # Script PowerShell: compilación, pruebas y despliegue E2E
├── build-test-deploy.sh             # Script Bash: compilación, pruebas y despliegue E2E
├── README.md                        # Especificación funcional y guía operativa
│
├── backend-rust/                    # Core API y Workers en Rust
│   ├── Cargo.toml                   # Dependencias (axum, tokio, rdkafka, serde, sqlx)
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                  # Punto de entrada y servidor HTTP/SSE
│       ├── config.rs                # Lectura de variables de entorno
│       ├── api/
│       │   ├── handlers.rs          # Endpoints REST (ingesta GPS y disparador de carga)
│       │   └── sse.rs               # Stream en tiempo real hacia Angular
│       ├── kafka/
│       │   ├── producer.rs          # Publicación en 'gps-telemetry' y 'route-alerts'
│       │   └── consumer.rs          # Evaluación de geocercas, alertas y tests unitarios
│       └── simulator/
│           └── fleet_gen.rs         # Generador de tráfico masivo para pruebas
│
├── orchestrator-airflow/            # Automatización de procesos Batch
│   ├── Dockerfile                   # Imagen extendida con providers de Postgres/HTTP
│   ├── dags/
│   │   ├── daily_fleet_kpi.py       # DAG: Cálculo de puntualidad, bonos y limpieza
│   │   └── route_optimization.py    # DAG: Asignación de rutas del día siguiente
│   └── scripts/
│       └── data_aggregator.py       # Lógica de transformación de datos en PostgreSQL
│
├── frontend-angular/                # Monitoreo en Angular 21
│   ├── angular.json
│   ├── package.json
│   ├── nginx.conf                   # Reverse proxy para /api/ (Rust) y /airflow-api/
│   ├── Dockerfile
│   └── src/
│       └── app/
│           ├── core/
│           │   ├── services/
│           │   │   ├── telemetry.service.ts  # Conexión SSE con Rust usando Signals
│           │   │   └── airflow.service.ts    # Consulta y disparo API REST de Airflow
│           │   └── models/
│           │       └── fleet.model.ts        # Interfaces TypeScript
│           └── features/
│               ├── live-map/                 # Radar SVG de geocercas y estado de flotilla
│               ├── alerts-feed/              # Panel reactivo de incidencias
│               └── batch-monitor/            # Estado, control de DAGs y tabla de KPIs
│
└── infra/                           # Scripts de inicialización y Proxies WAF
    ├── postgres/
    │   └── init.sql                 # Tablas para historial de rutas, alertas, entregas y KPIs
    └── proxy/
        └── backend-proxy.conf       # Reverse Proxy y reglas WAF de la capa Backend
```

---

## 3. Puertos Instanciados por Aplicación y Servicio

Al desplegar la solución completa mediante Docker Compose, se exponen e instancian los siguientes puertos en el host (`localhost`) y en la red interna de contenedores:

| Servicio / Contenedor | Tecnología | Puerto Host | Puerto Interno | URL / Endpoint Principal | Credenciales / Notas |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **`frontend-angular`** (`swiftroute-angular-ui`) | Angular 21 + Nginx 1.27 (Frontend Reverse Proxy & WAF) | **`4200`** | `80` (Docker) / `4200` (`ng serve`) | `http://localhost:4200` | Reverse Proxy con Rate Limit, CSP y WAF hacia `backend-proxy` (`/api/` y `/airflow-api/`) |
| **`backend-proxy`** (`swiftroute-backend-proxy`) | Nginx 1.27 (Backend Reverse Proxy & WAF) | **`8080`** | `8080` | `http://localhost:8080/swagger-ui`<br>`http://localhost:8080/api/health`<br>`http://localhost:8080/api/sse/telemetry` | Protege a `backend-rust` y `airflow` con Rate Limit, WAF (anti-SQLi/XSS/Traversal) e inyección server-side de credenciales de Airflow |
| **`backend-rust`** (`swiftroute-rust-api`) | Rust (Axum + Tokio + RdKafka + SQLx) | *(Red Interna)* | `8080` (`expose`) | `http://backend-rust:8080` (vía `backend-proxy`) | Ejecutado con usuario no-root (`swiftroute`), CORS estricto, límite de payload 64KB y validación de coordenadas GPS |
| **`airflow`** (`swiftroute-airflow`) | Apache Airflow 2.10 (Standalone) | **`8081`** | `8080` | `http://localhost:8081`<br>`http://localhost:8080/api/airflow/dags` | Definidas en `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`, inyectadas server-side por `backend-proxy`) |
| **`kafka`** (`swiftroute-kafka`) | Apache Kafka 3.8 (Modo KRaft) | **`127.0.0.1:9092`** (solo local) | `9092` (Broker)<br>`9093` (Controller) | `localhost:9092` / `kafka:29092` | Listener externo limitado a loopback; `AUTO_CREATE_TOPICS=false`, aprovisionamiento explícito de `gps-telemetry` y `route-alerts` (3 particiones) |
| **`postgres`** (`swiftroute-postgres`) | PostgreSQL 16 Alpine | **`5432`** | `5432` | `postgresql://localhost:5432/swiftroute` | Definidas en `.env` (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`) |

### Endpoints del Core API en Rust (`http://localhost:8080`)

- `GET  /swagger-ui` (y `GET /api/docs`): Interfaz interactiva **Swagger UI** para explorar y probar todos los endpoints.
- `GET  /api/openapi.json`: Especificación completa **OpenAPI 3.0.3** en formato JSON.
- `GET  /api/health`: Estado del servidor, simulador y conexión a PostgreSQL.
- `GET  /api/sse/telemetry`: Flujo **Server-Sent Events (SSE)** en tiempo real con eventos `telemetry` y `alert`.
- `GET  /api/fleet/state`: Snapshot actual de todos los vehículos activos y alertas recientes.
- `POST /api/telemetry`: Ingesta manual de coordenadas GPS (`driver_id`, `latitude`, `longitude`, `speed_kmh`).
- `POST /api/simulator/trigger`: Disparo de simulaciones bajo demanda (`{"mode": "normal" | "deviation" | "idle" | "toggle_auto", "burst_count": 1}`).
- `GET  /api/alerts`: Historial reciente de alertas de geocerca (`>500m`) e inactividad (`>10 min`).
- `GET  /api/kpis`: Reporte consolidado de KPIs y liquidaciones diarias por zona (`daily_zone_kpis`).

---

## 4. Guía de Compilación, Pruebas y Despliegue

El proyecto incluye dos scripts automatizados que ejecutan el ciclo completo de **compilación (`build`)**, **pruebas unitarias y validación (`test`)**, **despliegue (`deploy`)** y **pruebas de humo E2E (`smoke`)**:

- **Windows (PowerShell)**: `build-test-deploy.ps1`
- **Linux / macOS / WSL / Git Bash**: `build-test-deploy.sh`

### 4.1. Ejecución en Windows (PowerShell)

```powershell
# 1. Ejecutar el ciclo completo (Compilar + Probar + Desplegar + Smoke Tests E2E)
.\build-test-deploy.ps1 -Action all

# 2. Compilar y desplegar directamente usando los contenedores Docker multi-stage
#    (omite cargo/npm locales en el host)
.\build-test-deploy.ps1 -Action all -SkipLocalBuild

# 3. Ejecutar etapas individuales:
.\build-test-deploy.ps1 -Action build   # Compila Rust, Angular 21 e imágenes Docker
.\build-test-deploy.ps1 -Action test    # Ejecuta tests unitarios Haversine (Rust), py_compile (Airflow) y validación Angular
.\build-test-deploy.ps1 -Action deploy  # Levanta docker compose up -d --build y corre Smoke Tests
.\build-test-deploy.ps1 -Action smoke   # Verifica endpoints HTTP y simula desvío (>500m) y parada (>10m)
.\build-test-deploy.ps1 -Action down    # Detiene contenedores y limpia volúmenes (docker compose down -v)
```

### 4.2. Ejecución en Linux / macOS / Git Bash (`sh`)

```bash
chmod +x build-test-deploy.sh

# 1. Ejecutar el ciclo completo (Compilar + Probar + Desplegar + Smoke Tests E2E)
./build-test-deploy.sh all

# 2. Compilar únicamente dentro de Docker (sin requerir Rust o Node instalados en el host)
./build-test-deploy.sh all --skip-local

# 3. Ejecutar etapas individuales:
./build-test-deploy.sh build
./build-test-deploy.sh test
./build-test-deploy.sh deploy
./build-test-deploy.sh smoke
./build-test-deploy.sh down
```

### 4.3. Compilación y Pruebas Manuales por Componente

Si deseas compilar o probar cada componente de forma aislada durante el desarrollo:

1. **Backend Rust (`backend-rust/`)**:
   ```bash
   cd backend-rust
   cargo check
   cargo test -- --nocapture
   cargo run
   ```
2. **Frontend Angular 21 (`frontend-angular/`)**:
   ```bash
   cd frontend-angular
   npm install --legacy-peer-deps
   npm run build
   npm start   # Levanta servidor de desarrollo en http://localhost:4200
   ```
3. **Orquestador Apache Airflow (`orchestrator-airflow/`)**:
   ```bash
   python -m py_compile orchestrator-airflow/scripts/data_aggregator.py \
                        orchestrator-airflow/dags/daily_fleet_kpi.py \
                        orchestrator-airflow/dags/route_optimization.py
   ```
4. **Infraestructura Completa con Docker Compose**:
   ```bash
   docker compose up -d --build
   docker compose ps
   docker compose logs -f backend-rust
   ```

---

## 5. Notas Técnicas de Implementación

- **Seguridad y CORS en Airflow**: En `docker-compose.yml` se habilitó `basic_auth` (`admin:admin`) y cabeceras CORS en la API REST de Airflow. Además, el contenedor de Angular incluye un proxy en `nginx.conf` (`/airflow-api/` -> `http://airflow:8080/api/v1/`) para que el dashboard consulte el estado de los DAGs o dispare ejecuciones manuales (`POST /api/v1/dags/{dag_id}/dagRuns`) sin bloqueos de navegador.
- **Conexión en Vivo vía SSE + Angular 21 Signals**: Para evitar la sobrecarga de WebSockets bidireccionales cuando el flujo es servidor a cliente, Rust expone `GET /api/sse/telemetry` mediante Axum SSE y Angular 21 lo consume con `EventSource` nativo actualizando `signal()` y `computed()` en tiempo real.
