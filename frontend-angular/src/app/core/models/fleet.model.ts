export interface GpsTelemetry {
  driver_id: string;
  driver_name?: string;
  zone?: string;
  latitude: number;
  longitude: number;
  speed_kmh: number;
  delivery_status: 'IN_TRANSIT' | 'DELIVERING' | 'OFF_ROUTE' | 'IDLE_STOPPED' | string;
  assigned_lat?: number;
  assigned_lon?: number;
  delivery_lat?: number;
  delivery_lon?: number;
  deviation_meters?: number;
  idle_minutes?: number;
  timestamp: string;
}

export interface RouteAlert {
  alert_id: string;
  driver_id: string;
  driver_name: string;
  zone: string;
  alert_type: 'ROUTE_DEVIATION' | 'UNAUTHORIZED_STOP' | string;
  severity: 'WARNING' | 'CRITICAL' | string;
  message: string;
  latitude: number;
  longitude: number;
  metric_value: number;
  created_at: string;
}

export interface LiveStreamEvent {
  event_type: 'telemetry' | 'alert';
  data: GpsTelemetry | RouteAlert;
}

export interface FleetStatusSnapshot {
  simulator_active: boolean;
  active_drivers: number;
  total_recent_alerts: number;
  vehicles: GpsTelemetry[];
  alerts: RouteAlert[];
}

export interface DailyZoneKpi {
  report_date: string;
  zone: string;
  total_deliveries: number;
  successful_deliveries: number;
  on_time_rate: number;
  total_alerts: number;
  driver_payout_usd: number;
  computed_at: string;
  center_lat?: number;
  center_lon?: number;
}

export interface AirflowDagInfo {
  dag_id: string;
  description: string;
  is_paused: boolean;
  is_active: boolean;
  schedule_interval: string;
  last_run_state?: 'success' | 'running' | 'failed' | 'queued';
  last_run_date?: string;
  tags: string[];
}
