import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {
  DailyZoneKpi,
  FleetStatusSnapshot,
  GpsTelemetry,
  LiveStreamEvent,
  RouteAlert,
} from '../models/fleet.model';

@Injectable({
  providedIn: 'root',
})
export class TelemetryService {
  private readonly http = inject(HttpClient);

  // Enruta todas las peticiones por el Reverse Proxy del Frontend (/api -> frontend nginx -> backend-proxy -> rust)
  private readonly apiBase = '/api';

  private eventSource: EventSource | null = null;

  // Angular 21 Signals para estado reactivo en tiempo real
  readonly vehiclesMap = signal<Record<string, GpsTelemetry>>({});
  readonly alerts = signal<RouteAlert[]>([]);
  readonly kpis = signal<DailyZoneKpi[]>([]);
  readonly sseConnected = signal<boolean>(false);
  readonly simulatorActive = signal<boolean>(true);
  readonly totalEventsProcessed = signal<number>(0);

  readonly vehicles = computed(() =>
    Object.values(this.vehiclesMap()).sort((a, b) =>
      a.driver_id.localeCompare(b.driver_id)
    )
  );

  readonly activeCount = computed(() => this.vehicles().length);

  readonly offRouteCount = computed(
    () =>
      this.vehicles().filter((v) => (v.deviation_meters ?? 0) > 500).length
  );

  readonly stoppedCount = computed(
    () =>
      this.vehicles().filter(
        (v) => v.speed_kmh <= 0.5 && (v.idle_minutes ?? 0) >= 10
      ).length
  );

  readonly avgSpeedKmh = computed(() => {
    const list = this.vehicles();
    if (!list.length) return 0;
    const sum = list.reduce((acc, v) => acc + v.speed_kmh, 0);
    return Math.round((sum / list.length) * 10) / 10;
  });

  // Agregaciones derivadas de GET http://localhost:8080/api/kpis
  readonly totalKpiDeliveries = computed(() =>
    this.kpis().reduce((acc, k) => acc + k.total_deliveries, 0)
  );

  readonly totalKpiSuccessful = computed(() =>
    this.kpis().reduce((acc, k) => acc + k.successful_deliveries, 0)
  );

  readonly avgKpiOnTimeRate = computed(() => {
    const list = this.kpis();
    if (!list.length) return 0;
    const sum = list.reduce((acc, k) => acc + k.on_time_rate, 0);
    return Math.round((sum / list.length) * 10) / 10;
  });

  readonly totalKpiPayoutUsd = computed(() =>
    this.kpis().reduce((acc, k) => acc + k.driver_payout_usd, 0)
  );

  readonly selectedZoneFilter = signal<string | null>(null);

  private readonly zoneCoordinates: Record<string, { lat: number; lon: number }> = {
    'Norte-Industrial': { lat: 19.4978, lon: -99.1269 },
    'Centro-Historico': { lat: 19.4326, lon: -99.1332 },
    'Sur-Coyoacan': { lat: 19.3467, lon: -99.1617 },
    'Poniente-SantaFe': { lat: 19.3663, lon: -99.2612 },
    'Oriente-Aeropuerto': { lat: 19.4361, lon: -99.0719 },
    'Centro-Roma': { lat: 19.4194, lon: -99.1626 },
  };

  constructor() {
    this.loadInitialSnapshot();
    this.loadDailyKpis();
    this.connectSseStream();
  }

  loadInitialSnapshot(): void {
    this.http.get<FleetStatusSnapshot>(`${this.apiBase}/fleet/state`).subscribe({
      next: (snap) => {
        const map: Record<string, GpsTelemetry> = {};
        for (const v of snap.vehicles) {
          map[v.driver_id] = v;
        }
        this.vehiclesMap.set(map);
        this.alerts.set(snap.alerts);
        this.simulatorActive.set(snap.simulator_active);
      },
      error: () => {
        // Cargar datos semilla de demostración si el backend aún está iniciando
        this.seedDemoStateIfOffline();
      },
    });
  }

  loadDailyKpis(): void {
    this.http.get<DailyZoneKpi[]>(`${this.apiBase}/kpis`).subscribe({
      next: (rows) => {
        const enriched = rows.map((r) => ({
          ...r,
          center_lat: this.zoneCoordinates[r.zone]?.lat ?? 19.4326,
          center_lon: this.zoneCoordinates[r.zone]?.lon ?? -99.1332,
        }));
        this.kpis.set(enriched);
      },
      error: () => {
        if (!this.kpis().length) {
          this.seedDemoStateIfOffline();
        }
      },
    });
  }

  /**
   * Conecta con el endpoint Server-Sent Events (SSE) de Axum (Rust) usando EventSource nativo
   * y actualiza los Signals de Angular 21 en tiempo real.
   */
  connectSseStream(): void {
    if (typeof EventSource === 'undefined') return;
    this.disconnectSseStream();

    const url = `${this.apiBase}/sse/telemetry`;
    this.eventSource = new EventSource(url);

    this.eventSource.onopen = () => {
      this.sseConnected.set(true);
    };

    this.eventSource.onmessage = (event) => {
      try {
        const parsed: LiveStreamEvent = JSON.parse(event.data);
        this.totalEventsProcessed.update((n) => n + 1);

        if (parsed.event_type === 'telemetry') {
          const telemetry = parsed.data as GpsTelemetry;
          this.vehiclesMap.update((prev) => ({
            ...prev,
            [telemetry.driver_id]: telemetry,
          }));
        } else if (parsed.event_type === 'alert') {
          const alert = parsed.data as RouteAlert;
          this.alerts.update((prev) => [alert, ...prev.slice(0, 49)]);
        }
      } catch {
        // Ignorar mensajes keep-alive
      }
    };

    this.eventSource.onerror = () => {
      this.sseConnected.set(false);
    };
  }

  disconnectSseStream(): void {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
      this.sseConnected.set(false);
    }
  }

  triggerSimulation(
    mode: 'normal' | 'deviation' | 'idle' | 'toggle_auto',
    burstCount = 1
  ): void {
    this.http
      .post<{ simulator_active?: boolean }>(
        `${this.apiBase}/simulator/trigger`,
        {
          mode,
          burst_count: burstCount,
        }
      )
      .subscribe({
        next: (res) => {
          if (typeof res.simulator_active === 'boolean') {
            this.simulatorActive.set(res.simulator_active);
          }
        },
        error: () => {
          // Simulación local en caso de demo sin backend levantado
          this.simulateLocalBurst(mode);
        },
      });
  }

  clearAlerts(): void {
    this.alerts.set([]);
  }

  private seedDemoStateIfOffline(): void {
    const now = new Date().toISOString();
    const initial: GpsTelemetry[] = [
      {
        driver_id: 'DRV-001',
        driver_name: 'Carlos Mendoza',
        zone: 'Norte-Industrial',
        latitude: 19.4985,
        longitude: -99.1261,
        speed_kmh: 44.2,
        delivery_status: 'IN_TRANSIT',
        assigned_lat: 19.4978,
        assigned_lon: -99.1269,
        deviation_meters: 112.4,
        idle_minutes: 0,
        timestamp: now,
      },
      {
        driver_id: 'DRV-002',
        driver_name: 'Laura Villarreal',
        zone: 'Centro-Historico',
        latitude: 19.4331,
        longitude: -99.1328,
        speed_kmh: 31.8,
        delivery_status: 'IN_TRANSIT',
        assigned_lat: 19.4326,
        assigned_lon: -99.1332,
        deviation_meters: 69.5,
        idle_minutes: 0,
        timestamp: now,
      },
      {
        driver_id: 'DRV-003',
        driver_name: 'Miguel Ángel Soto',
        zone: 'Sur-Coyoacan',
        latitude: 19.3471,
        longitude: -99.1612,
        speed_kmh: 38.0,
        delivery_status: 'IN_TRANSIT',
        assigned_lat: 19.3467,
        assigned_lon: -99.1617,
        deviation_meters: 72.1,
        idle_minutes: 0,
        timestamp: now,
      },
      {
        driver_id: 'DRV-004',
        driver_name: 'Sofía Hernández',
        zone: 'Poniente-SantaFe',
        latitude: 19.3669,
        longitude: -99.2608,
        speed_kmh: 49.5,
        delivery_status: 'DELIVERING',
        assigned_lat: 19.3663,
        assigned_lon: -99.2612,
        deviation_meters: 78.9,
        idle_minutes: 0,
        timestamp: now,
      },
    ];
    const map: Record<string, GpsTelemetry> = {};
    for (const v of initial) map[v.driver_id] = v;
    this.vehiclesMap.set(map);

    this.kpis.set([
      {
        report_date: now.slice(0, 10),
        zone: 'Norte-Industrial',
        total_deliveries: 24,
        successful_deliveries: 23,
        on_time_rate: 95.8,
        total_alerts: 1,
        driver_payout_usd: 145.5,
        computed_at: now,
        center_lat: 19.4978,
        center_lon: -99.1269,
      },
      {
        report_date: now.slice(0, 10),
        zone: 'Centro-Historico',
        total_deliveries: 31,
        successful_deliveries: 28,
        on_time_rate: 90.3,
        total_alerts: 3,
        driver_payout_usd: 172.0,
        computed_at: now,
        center_lat: 19.4326,
        center_lon: -99.1332,
      },
      {
        report_date: now.slice(0, 10),
        zone: 'Sur-Coyoacan',
        total_deliveries: 19,
        successful_deliveries: 17,
        on_time_rate: 89.4,
        total_alerts: 2,
        driver_payout_usd: 118.75,
        computed_at: now,
        center_lat: 19.3467,
        center_lon: -99.1617,
      },
      {
        report_date: now.slice(0, 10),
        zone: 'Poniente-SantaFe',
        total_deliveries: 22,
        successful_deliveries: 22,
        on_time_rate: 100.0,
        total_alerts: 0,
        driver_payout_usd: 160.0,
        computed_at: now,
        center_lat: 19.3663,
        center_lon: -99.2612,
      },
      {
        report_date: now.slice(0, 10),
        zone: 'Oriente-Aeropuerto',
        total_deliveries: 27,
        successful_deliveries: 25,
        on_time_rate: 92.6,
        total_alerts: 2,
        driver_payout_usd: 154.2,
        computed_at: now,
        center_lat: 19.4361,
        center_lon: -99.0719,
      },
    ]);
  }

  private simulateLocalBurst(mode: string): void {
    const now = new Date().toISOString();
    if (mode === 'deviation') {
      this.vehiclesMap.update((m) => ({
        ...m,
        'DRV-001': {
          ...(m['DRV-001'] || {
            driver_id: 'DRV-001',
            driver_name: 'Carlos Mendoza',
            zone: 'Norte-Industrial',
          }),
          latitude: 19.5046,
          longitude: -99.1237,
          speed_kmh: 56.4,
          delivery_status: 'OFF_ROUTE',
          deviation_meters: 824.6,
          idle_minutes: 0,
          timestamp: now,
        },
      }));
      this.alerts.update((list) => [
        {
          alert_id: crypto.randomUUID(),
          driver_id: 'DRV-001',
          driver_name: 'Carlos Mendoza',
          zone: 'Norte-Industrial',
          alert_type: 'ROUTE_DEVIATION',
          severity: 'WARNING',
          message:
            'Desvío de ruta detectado: Carlos Mendoza se alejó 825m de su geocerca asignada en Norte-Industrial',
          latitude: 19.5046,
          longitude: -99.1237,
          metric_value: 824.6,
          created_at: now,
        },
        ...list,
      ]);
    } else if (mode === 'idle') {
      this.vehiclesMap.update((m) => ({
        ...m,
        'DRV-003': {
          ...(m['DRV-003'] || {
            driver_id: 'DRV-003',
            driver_name: 'Miguel Ángel Soto',
            zone: 'Sur-Coyoacan',
          }),
          latitude: 19.3488,
          longitude: -99.1636,
          speed_kmh: 0.0,
          delivery_status: 'IDLE_STOPPED',
          deviation_meters: 290.0,
          idle_minutes: 12.5,
          timestamp: now,
        },
      }));
      this.alerts.update((list) => [
        {
          alert_id: crypto.randomUUID(),
          driver_id: 'DRV-003',
          driver_name: 'Miguel Ángel Soto',
          zone: 'Sur-Coyoacan',
          alert_type: 'UNAUTHORIZED_STOP',
          severity: 'CRITICAL',
          message:
            'Parada no autorizada: Miguel Ángel Soto lleva 12.5 min detenido (0 km/h) a 410m del punto de entrega',
          latitude: 19.3488,
          longitude: -99.1636,
          metric_value: 12.5,
          created_at: now,
        },
        ...list,
      ]);
    }
  }
}
