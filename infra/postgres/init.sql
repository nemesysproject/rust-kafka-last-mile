-- ============================================================
-- SwiftRoute Last-Mile Delivery Schema & Seed Data
-- ============================================================

-- 1. Repartidores y Rutas Asignadas
CREATE TABLE IF NOT EXISTS drivers (
    driver_id VARCHAR(50) PRIMARY KEY,
    driver_name VARCHAR(100) NOT NULL,
    vehicle_plate VARCHAR(20) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    assigned_lat DOUBLE PRECISION NOT NULL,
    assigned_lon DOUBLE PRECISION NOT NULL,
    delivery_lat DOUBLE PRECISION NOT NULL,
    delivery_lon DOUBLE PRECISION NOT NULL,
    status VARCHAR(30) DEFAULT 'IN_TRANSIT',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Historial de Telemetría GPS
CREATE TABLE IF NOT EXISTS gps_telemetry (
    id BIGSERIAL PRIMARY KEY,
    driver_id VARCHAR(50) NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmh DOUBLE PRECISION NOT NULL,
    delivery_status VARCHAR(30) NOT NULL,
    deviation_meters DOUBLE PRECISION DEFAULT 0.0,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_driver_time ON gps_telemetry(driver_id, recorded_at DESC);

-- 3. Alertas de Geocercas e Inactividad ('route-alerts')
CREATE TABLE IF NOT EXISTS route_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    driver_id VARCHAR(50) NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL, -- 'ROUTE_DEVIATION' | 'UNAUTHORIZED_STOP'
    severity VARCHAR(20) NOT NULL,   -- 'WARNING' | 'CRITICAL'
    message TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL, -- metros de desvío o minutos inactivo
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_created ON route_alerts(created_at DESC);

-- 4. Entregas Completadas (insumo para Cierre Diario Batch en Airflow)
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id BIGSERIAL PRIMARY KEY,
    driver_id VARCHAR(50) NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    zone VARCHAR(50) NOT NULL,
    package_code VARCHAR(40) NOT NULL,
    status VARCHAR(30) NOT NULL, -- 'DELIVERED' | 'DELAYED' | 'FAILED'
    scheduled_time TIMESTAMPTZ NOT NULL,
    delivered_time TIMESTAMPTZ,
    distance_km DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. KPIs Diarios y Liquidación por Zona / Repartidor (generados por Airflow)
CREATE TABLE IF NOT EXISTS daily_zone_kpis (
    kpi_id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    zone VARCHAR(50) NOT NULL,
    total_deliveries INT NOT NULL,
    successful_deliveries INT NOT NULL,
    on_time_rate DOUBLE PRECISION NOT NULL,
    total_alerts INT NOT NULL,
    driver_payout_usd NUMERIC(10, 2) NOT NULL,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(report_date, zone)
);

-- 6. Planes de Optimización de Rutas del Día Siguiente (generados por Airflow)
CREATE TABLE IF NOT EXISTS route_optimizations (
    optimization_id BIGSERIAL PRIMARY KEY,
    target_date DATE NOT NULL,
    driver_id VARCHAR(50) NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    assigned_zone VARCHAR(50) NOT NULL,
    recommended_waypoints JSONB NOT NULL,
    estimated_saving_km DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(target_date, driver_id)
);

-- ============================================================
-- Datos Semilla Iniciales (Flotilla CDMX / Metropolitana)
-- ============================================================
INSERT INTO drivers (driver_id, driver_name, vehicle_plate, zone, assigned_lat, assigned_lon, delivery_lat, delivery_lon, status)
VALUES
    ('DRV-001', 'Carlos Mendoza', 'MX-482-A', 'Norte-Industrial', 19.4978, -99.1269, 19.5010, -99.1240, 'IN_TRANSIT'),
    ('DRV-002', 'Laura Villarreal', 'MX-910-B', 'Centro-Historico', 19.4326, -99.1332, 19.4350, -99.1300, 'IN_TRANSIT'),
    ('DRV-003', 'Miguel Ángel Soto', 'MX-204-C', 'Sur-Coyoacan', 19.3467, -99.1617, 19.3500, -99.1600, 'IN_TRANSIT'),
    ('DRV-004', 'Sofía Hernández', 'MX-775-D', 'Poniente-SantaFe', 19.3663, -99.2612, 19.3690, -99.2580, 'DELIVERING'),
    ('DRV-005', 'Ricardo Téllez', 'MX-631-E', 'Oriente-Aeropuerto', 19.4361, -99.0719, 19.4400, -99.0690, 'IN_TRANSIT'),
    ('DRV-006', 'Elena Gómez', 'MX-119-F', 'Centro-Roma', 19.4194, -99.1626, 19.4220, -99.1600, 'IN_TRANSIT')
ON CONFLICT (driver_id) DO NOTHING;

INSERT INTO deliveries (driver_id, zone, package_code, status, scheduled_time, delivered_time, distance_km)
VALUES
    ('DRV-001', 'Norte-Industrial', 'PKG-9001', 'DELIVERED', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 hours 40 minutes', 12.4),
    ('DRV-001', 'Norte-Industrial', 'PKG-9002', 'DELIVERED', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour 55 minutes', 8.1),
    ('DRV-002', 'Centro-Historico', 'PKG-9003', 'DELIVERED', NOW() - INTERVAL '4 hours', NOW() - INTERVAL '3 hours 10 minutes', 6.5),
    ('DRV-003', 'Sur-Coyoacan', 'PKG-9004', 'DELAYED', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour', 15.2),
    ('DRV-004', 'Poniente-SantaFe', 'PKG-9005', 'DELIVERED', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '50 minutes', 19.0),
    ('DRV-005', 'Oriente-Aeropuerto', 'PKG-9006', 'DELIVERED', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 hours 50 minutes', 11.3),
    ('DRV-006', 'Centro-Roma', 'PKG-9007', 'DELIVERED', NOW() - INTERVAL '90 minutes', NOW() - INTERVAL '80 minutes', 5.8);

INSERT INTO daily_zone_kpis (report_date, zone, total_deliveries, successful_deliveries, on_time_rate, total_alerts, driver_payout_usd)
VALUES
    (CURRENT_DATE, 'Norte-Industrial', 24, 23, 95.8, 1, 145.50),
    (CURRENT_DATE, 'Centro-Historico', 31, 28, 90.3, 3, 172.00),
    (CURRENT_DATE, 'Sur-Coyoacan', 19, 17, 89.4, 2, 118.75),
    (CURRENT_DATE, 'Poniente-SantaFe', 22, 22, 100.0, 0, 160.00),
    (CURRENT_DATE, 'Oriente-Aeropuerto', 27, 25, 92.6, 2, 154.20)
ON CONFLICT (report_date, zone) DO NOTHING;
