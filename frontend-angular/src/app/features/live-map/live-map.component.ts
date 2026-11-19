import {
  AfterViewInit,
  Component,
  ElementRef,
  OnDestroy,
  ViewChild,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import * as L from 'leaflet';
import { TelemetryService } from '../../core/services/telemetry.service';
import { DailyZoneKpi, GpsTelemetry } from '../../core/models/fleet.model';

@Component({
  selector: 'app-live-map',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="glass-panel p-3 p-lg-4 h-100 d-flex flex-column gap-3">
      <!-- Cabecera del Mapa Admin con Controles Bootstrap 5 -->
      <div class="d-flex flex-wrap justify-content-between align-items-center gap-2">
        <div>
          <div class="d-flex align-items-center gap-2">
            <span class="badge rounded-pill text-bg-info bg-opacity-25 text-info border border-info border-opacity-50">
              <i class="bi bi-geo-alt-fill me-1"></i>MAPA GEOSPACIAL DE KPIs &amp; FLOTILLA
            </span>
            <span class="small text-secondary font-mono">GET /api/kpis + /api/sse/telemetry</span>
          </div>
          <h5 class="mb-0 mt-1 fw-bold text-white">
            Monitoreo Territorial de Zonas KPI y Geocercas en Vivo
          </h5>
        </div>

        <div class="btn-group btn-group-sm" role="group">
          <button
            type="button"
            class="btn btn-glass"
            (click)="resetMapView()"
            title="Centrar mapa en todas las zonas KPI">
            <i class="bi bi-arrows-fullscreen me-1"></i>Vista General
          </button>
          <button
            type="button"
            class="btn btn-glass-cyan"
            (click)="telemetry.triggerSimulation('normal', 1)">
            <i class="bi bi-broadcast me-1"></i>Pulso GPS
          </button>
          <button
            type="button"
            class="btn btn-glass-amber"
            (click)="telemetry.triggerSimulation('deviation', 1)">
            <i class="bi bi-exclamation-triangle me-1"></i>Desvío (&gt;500m)
          </button>
          <button
            type="button"
            class="btn btn-glass-rose"
            (click)="telemetry.triggerSimulation('idle', 1)">
            <i class="bi bi-sign-stop me-1"></i>Parada (&gt;10m)
          </button>
        </div>
      </div>

      <!-- Cinta de Zonas Monitoreadas en http://localhost:8080/api/kpis -->
      <div class="row g-2">
        @for (kpi of telemetry.kpis(); track kpi.zone) {
          <div class="col-12 col-sm-6 col-xl">
            <div
              class="glass-card p-2 px-3 cursor-pointer h-100"
              [class.active-zone]="telemetry.selectedZoneFilter() === kpi.zone"
              (click)="focusKpiZone(kpi)">
              <div class="d-flex justify-content-between align-items-center">
                <span class="fw-bold small text-info text-truncate">
                  <i class="bi bi-pin-map-fill me-1"></i>{{ kpi.zone }}
                </span>
                <span
                  class="badge rounded-pill"
                  [class.text-bg-success]="kpi.on_time_rate >= 95"
                  [class.text-bg-warning]="kpi.on_time_rate < 95">
                  {{ kpi.on_time_rate | number:'1.1-1' }}%
                </span>
              </div>
              <div class="d-flex justify-content-between align-items-baseline mt-1 small">
                <span class="text-secondary">
                  Entregas: <strong class="text-white font-mono">{{ kpi.successful_deliveries }}/{{ kpi.total_deliveries }}</strong>
                </span>
                <span class="text-success fw-bold font-mono">
                  \${{ kpi.driver_payout_usd | number:'1.0-0' }}
                </span>
              </div>
              <div class="progress mt-1" style="height: 4px; background: rgba(255,255,255,0.08);">
                <div
                  class="progress-bar"
                  [class.bg-info]="kpi.on_time_rate >= 95"
                  [class.bg-warning]="kpi.on_time_rate < 95"
                  [style.width.%]="kpi.on_time_rate"></div>
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Contenedor del Mapa Interactivo Leaflet con Overlay Glass -->
      <div class="position-relative rounded-4 overflow-hidden border border-light border-opacity-10" style="min-height: 360px;">
        <div #mapContainer class="w-100" style="height: 370px;"></div>

        <!-- Overlay Flotante Glass con Leyenda de Capas -->
        <div
          class="position-absolute bottom-0 start-0 m-3 p-2 px-3 glass-card d-flex flex-wrap gap-3 align-items-center small"
          style="z-index: 500;">
          <span class="d-flex align-items-center gap-1">
            <span class="legend-dot bg-info"></span> Zona KPI (<code>/api/kpis</code>)
          </span>
          <span class="d-flex align-items-center gap-1">
            <span class="legend-dot bg-success"></span> En Ruta (&le;500m)
          </span>
          <span class="d-flex align-items-center gap-1">
            <span class="legend-dot bg-warning"></span> Desvío Geocerca (&gt;500m)
          </span>
          <span class="d-flex align-items-center gap-1">
            <span class="legend-dot bg-danger"></span> Parada No Autorizada (0 km/h)
          </span>
        </div>

        @if (telemetry.selectedZoneFilter()) {
          <div class="position-absolute top-0 end-0 m-3" style="z-index: 500;">
            <button
              class="btn btn-sm btn-glass-cyan rounded-pill px-3"
              (click)="resetMapView()">
              <i class="bi bi-funnel-fill me-1"></i>Zona: {{ telemetry.selectedZoneFilter() }} (Ver todas)
            </button>
          </div>
        }
      </div>

      <!-- Tabla Bootstrap 5 Glass de Unidades Monitoreadas -->
      <div class="table-responsive">
        <table class="table table-glass table-hover align-middle small">
          <thead>
            <tr>
              <th>Unidad / Repartidor</th>
              <th>Zona KPI</th>
              <th>Coordenadas GPS</th>
              <th>Velocidad</th>
              <th>Desvío Geocerca</th>
              <th>KPI Zona (Puntualidad / Pago)</th>
              <th>Estado</th>
            </tr>
          </thead>
          <tbody>
            @for (v of filteredVehicles(); track v.driver_id) {
              <tr
                class="cursor-pointer"
                [class.table-danger]="(v.deviation_meters ?? 0) > 500 || (v.speed_kmh <= 0.5 && (v.idle_minutes ?? 0) >= 10)"
                (click)="panToVehicle(v)">
                <td>
                  <div class="fw-bold text-white">{{ v.driver_id }}</div>
                  <div class="text-secondary" style="font-size: 0.75rem;">{{ v.driver_name || 'Repartidor Activo' }}</div>
                </td>
                <td>
                  <span class="badge badge-glass rounded-pill">
                    <i class="bi bi-geo me-1 text-info"></i>{{ v.zone || 'Metropolitana' }}
                  </span>
                </td>
                <td class="font-mono text-secondary">
                  {{ v.latitude | number:'1.4-4' }}, {{ v.longitude | number:'1.4-4' }}
                </td>
                <td class="font-mono">
                  <span class="fw-bold text-white">{{ v.speed_kmh | number:'1.1-1' }} km/h</span>
                  @if (v.speed_kmh <= 0.5 && (v.idle_minutes ?? 0) > 0) {
                    <span class="badge text-bg-danger ms-1">{{ v.idle_minutes | number:'1.0-1' }}m parado</span>
                  }
                </td>
                <td class="font-mono">
                  <span
                    class="badge rounded-pill"
                    [class.text-bg-success]="(v.deviation_meters ?? 0) <= 500"
                    [class.text-bg-danger]="(v.deviation_meters ?? 0) > 500">
                    {{ v.deviation_meters ?? 0 | number:'1.0-0' }} m
                  </span>
                </td>
                <td>
                  @if (getZoneKpi(v.zone); as zKpi) {
                    <div class="d-flex align-items-center gap-2 font-mono">
                      <span class="text-info fw-bold">{{ zKpi.on_time_rate | number:'1.1-1' }}%</span>
                      <span class="text-secondary">·</span>
                      <span class="text-success fw-bold">\${{ zKpi.driver_payout_usd | number:'1.2-2' }}</span>
                    </div>
                  } @else {
                    <span class="text-secondary font-mono">Sincronizando...</span>
                  }
                </td>
                <td>
                  <span
                    class="badge rounded-pill"
                    [class.text-bg-success]="v.delivery_status === 'IN_TRANSIT' || v.delivery_status === 'DELIVERING'"
                    [class.text-bg-warning]="v.delivery_status === 'OFF_ROUTE'"
                    [class.text-bg-danger]="v.delivery_status === 'IDLE_STOPPED'">
                    {{ v.delivery_status }}
                  </span>
                </td>
              </tr>
            }
          </tbody>
        </table>
      </div>
    </div>
  `,
  styles: [
    `
      .cursor-pointer {
        cursor: pointer;
      }
      .legend-dot {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
      }
    `,
  ],
})
export class LiveMapComponent implements AfterViewInit, OnDestroy {
  @ViewChild('mapContainer', { static: true })
  mapContainer!: ElementRef<HTMLDivElement>;

  readonly telemetry = inject(TelemetryService);
  readonly selectedDriver = signal<GpsTelemetry | null>(null);

  readonly filteredVehicles = computed(() => {
    const filter = this.telemetry.selectedZoneFilter();
    const all = this.telemetry.vehicles();
    if (!filter) return all;
    return all.filter((v) => v.zone === filter);
  });

  private map: L.Map | null = null;
  private kpiLayerGroup = L.layerGroup();
  private vehicleLayerGroup = L.layerGroup();

  constructor() {
    // Re-renderizar marcadores del mapa automáticamente cuando cambien los Signals de KPIs o Vehículos
    effect(() => {
      const kpis = this.telemetry.kpis();
      const vehicles = this.telemetry.vehicles();
      this.renderMapLayers(kpis, vehicles);
    });
  }

  ngAfterViewInit(): void {
    this.initLeafletMap();
    this.renderMapLayers(this.telemetry.kpis(), this.telemetry.vehicles());
  }

  ngOnDestroy(): void {
    if (this.map) {
      this.map.remove();
      this.map = null;
    }
  }

  getZoneKpi(zone?: string): DailyZoneKpi | undefined {
    if (!zone) return undefined;
    return this.telemetry.kpis().find((k) => k.zone === zone);
  }

  focusKpiZone(kpi: DailyZoneKpi): void {
    const current = this.telemetry.selectedZoneFilter();
    if (current === kpi.zone) {
      this.resetMapView();
      return;
    }
    this.telemetry.selectedZoneFilter.set(kpi.zone);
    if (this.map && kpi.center_lat && kpi.center_lon) {
      this.map.flyTo([kpi.center_lat, kpi.center_lon], 14, {
        duration: 0.9,
      });
    }
  }

  panToVehicle(v: GpsTelemetry): void {
    this.selectedDriver.set(v);
    if (this.map) {
      this.map.flyTo([v.latitude, v.longitude], 15, { duration: 0.8 });
    }
  }

  resetMapView(): void {
    this.telemetry.selectedZoneFilter.set(null);
    if (this.map) {
      this.map.flyTo([19.428, -99.155], 11, { duration: 0.9 });
    }
  }

  private initLeafletMap(): void {
    if (this.map) return;

    this.map = L.map(this.mapContainer.nativeElement, {
      center: [19.428, -99.155],
      zoom: 11,
      zoomControl: true,
      attributionControl: false,
    });

    // CartoDB Dark Matter Tiles para estética Glassmorphism oscura
    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      {
        maxZoom: 19,
        subdomains: 'abcd',
      }
    ).addTo(this.map);

    this.kpiLayerGroup.addTo(this.map);
    this.vehicleLayerGroup.addTo(this.map);
  }

  private renderMapLayers(
    kpis: DailyZoneKpi[],
    vehicles: GpsTelemetry[]
  ): void {
    if (!this.map) return;

    this.kpiLayerGroup.clearLayers();
    this.vehicleLayerGroup.clearLayers();

    // 1. Pintar Zonas Monitoreadas de http://localhost:8080/api/kpis
    for (const kpi of kpis) {
      const lat = kpi.center_lat ?? 19.4326;
      const lon = kpi.center_lon ?? -99.1332;
      const isHighPerf = kpi.on_time_rate >= 95;
      const zoneColor = isHighPerf ? '#38bdf8' : '#fbbf24';

      // Geocerca de la Zona KPI (radio proporcional al volumen de entregas)
      L.circle([lat, lon], {
        radius: 650 + kpi.total_deliveries * 18,
        color: zoneColor,
        weight: 1.8,
        fillColor: zoneColor,
        fillOpacity: 0.14,
        dashArray: '6, 4',
      })
        .bindPopup(
          `
          <div class="p-1">
            <div class="text-info fw-bold mb-1"><i class="bi bi-pin-map-fill me-1"></i>Zona KPI: ${kpi.zone}</div>
            <div class="small">Entregas Exitosas: <strong>${kpi.successful_deliveries} / ${kpi.total_deliveries}</strong></div>
            <div class="small">Puntualidad: <strong class="text-info">${kpi.on_time_rate}%</strong></div>
            <div class="small">Alertas Registradas: <strong class="text-warning">${kpi.total_alerts}</strong></div>
            <div class="small">Liquidación Repartidores: <strong class="text-success">$${kpi.driver_payout_usd.toFixed(2)} USD</strong></div>
          </div>
        `
        )
        .addTo(this.kpiLayerGroup);

      // Etiqueta flotante Glass sobre el hub de cada Zona KPI
      const badgeIcon = L.divIcon({
        className: 'kpi-div-icon',
        html: `
          <div class="kpi-map-badge">
            <strong>${kpi.zone}</strong>
            <span>${kpi.successful_deliveries}/${kpi.total_deliveries} · ${kpi.on_time_rate}% · $${kpi.driver_payout_usd}</span>
          </div>
        `,
        iconSize: [150, 38],
        iconAnchor: [75, -10],
      });

      L.marker([lat, lon], { icon: badgeIcon }).addTo(this.kpiLayerGroup);
    }

    // 2. Pintar Vehículos en Tiempo Real y su Vector de Geocerca (500m)
    for (const v of vehicles) {
      const isOffRoute = (v.deviation_meters ?? 0) > 500;
      const isStopped = v.speed_kmh <= 0.5;
      const pinBg = isStopped
        ? '#fb7185'
        : isOffRoute
        ? '#fbbf24'
        : '#34d399';

      // Línea entre la geocerca asignada y la posición GPS actual del repartidor
      if (v.assigned_lat && v.assigned_lon) {
        L.polyline(
          [
            [v.assigned_lat, v.assigned_lon],
            [v.latitude, v.longitude],
          ],
          {
            color: isOffRoute ? '#fb7185' : '#38bdf8',
            weight: isOffRoute ? 2.5 : 1.5,
            dashArray: isOffRoute ? '4, 4' : undefined,
            opacity: 0.8,
          }
        ).addTo(this.vehicleLayerGroup);
      }

      const driverIcon = L.divIcon({
        className: 'driver-div-icon',
        html: `<div class="driver-map-pin" style="background:${pinBg};">${v.driver_id.replace('DRV-00', 'D')}</div>`,
        iconSize: [26, 26],
        iconAnchor: [13, 13],
      });

      L.marker([v.latitude, v.longitude], { icon: driverIcon })
        .bindPopup(
          `
          <div class="p-1">
            <div class="fw-bold text-white">${v.driver_id} · ${v.driver_name || 'Repartidor'}</div>
            <div class="small text-info mb-1">Zona: ${v.zone || 'CDMX'}</div>
            <div class="small">Velocidad: <strong>${v.speed_kmh} km/h</strong></div>
            <div class="small">Desvío Geocerca: <strong>${Math.round(v.deviation_meters ?? 0)} m</strong></div>
            <div class="small">Estado: <strong>${v.delivery_status}</strong></div>
          </div>
        `
        )
        .addTo(this.vehicleLayerGroup);
    }
  }
}
