import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TelemetryService } from './core/services/telemetry.service';
import { LiveMapComponent } from './features/live-map/live-map.component';
import { AlertsFeedComponent } from './features/alerts-feed/alerts-feed.component';
import { BatchMonitorComponent } from './features/batch-monitor/batch-monitor.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    LiveMapComponent,
    AlertsFeedComponent,
    BatchMonitorComponent,
  ],
  template: `
    <div class="d-flex min-vh-100">
      <!-- Sidebar Admin Bootstrap 5 Glassmorphism -->
      <aside
        class="glass-sidebar d-none d-lg-flex flex-column justify-content-between p-3"
        style="width: 265px; flex-shrink: 0;">
        <div class="d-flex flex-column gap-4">
          <!-- Brand Header -->
          <div class="d-flex align-items-center gap-3 px-2 pt-1">
            <div
              class="rounded-3 d-flex align-items-center justify-content-center fw-bold text-dark shadow"
              style="width: 42px; height: 42px; background: linear-gradient(135deg, #38bdf8, #818cf8);">
              <i class="bi bi-truck-front-fill fs-5"></i>
            </div>
            <div>
              <div class="fw-bold text-white lh-1">SwiftRoute</div>
              <small class="text-info font-mono" style="font-size: 0.72rem;">ADMIN GLASS v2.1</small>
            </div>
          </div>

          <!-- Navegación Admin -->
          <div class="d-flex flex-column gap-1">
            <small class="text-uppercase text-secondary fw-semibold px-2 mb-1" style="font-size: 0.68rem; letter-spacing: 0.08em;">
              Centro de Control
            </small>
            <button
              type="button"
              class="btn text-start d-flex align-items-center gap-2 px-3 py-2 rounded-3"
              [class.btn-glass-cyan]="activeSection() === 'overview'"
              [class.btn-glass]="activeSection() !== 'overview'"
              (click)="activeSection.set('overview')">
              <i class="bi bi-speedometer2"></i>
              <span>Dashboard &amp; Mapa KPI</span>
            </button>
            <button
              type="button"
              class="btn text-start d-flex align-items-center justify-content-between px-3 py-2 rounded-3"
              [class.btn-glass-cyan]="activeSection() === 'alerts'"
              [class.btn-glass]="activeSection() !== 'alerts'"
              (click)="activeSection.set('alerts')">
              <span><i class="bi bi-exclamation-diamond me-2"></i>Alertas Kafka</span>
              <span class="badge rounded-pill text-bg-danger">{{ telemetry.alerts().length }}</span>
            </button>
            <button
              type="button"
              class="btn text-start d-flex align-items-center gap-2 px-3 py-2 rounded-3"
              [class.btn-glass-cyan]="activeSection() === 'batch'"
              [class.btn-glass]="activeSection() !== 'batch'"
              (click)="activeSection.set('batch')">
              <i class="bi bi-diagram-3"></i>
              <span>Airflow Batch &amp; KPIs</span>
            </button>
          </div>

          <!--Filtro Rápido de Zonas de http://localhost:8080/api/kpis -->
          <div class="d-flex flex-column gap-2">
            <div class="d-flex justify-content-between align-items-center px-2">
              <small class="text-uppercase text-secondary fw-semibold" style="font-size: 0.68rem; letter-spacing: 0.08em;">
                Zonas Monitoreadas (/api/kpis)
              </small>
              @if (telemetry.selectedZoneFilter()) {
                <button
                  class="btn btn-link btn-sm p-0 text-info text-decoration-none"
                  style="font-size: 0.72rem;"
                  (click)="telemetry.selectedZoneFilter.set(null)">
                  Todas
                </button>
              }
            </div>
            @for (kpi of telemetry.kpis(); track kpi.zone) {
              <div
                class="glass-card p-2 px-3 small cursor-pointer"
                [class.active-zone]="telemetry.selectedZoneFilter() === kpi.zone"
                (click)="toggleZoneFilter(kpi.zone)">
                <div class="d-flex justify-content-between align-items-center">
                  <span class="text-white fw-semibold text-truncate">{{ kpi.zone }}</span>
                  <span class="badge badge-glass font-mono">{{ kpi.on_time_rate }}%</span>
                </div>
                <div class="d-flex justify-content-between text-secondary mt-1" style="font-size: 0.72rem;">
                  <span>{{ kpi.successful_deliveries }}/{{ kpi.total_deliveries }} entregas</span>
                  <span class="text-success font-mono">\${{ kpi.driver_payout_usd }}</span>
                </div>
              </div>
            }
          </div>
        </div>

        <!-- Enlaces Rápidos a Servicios Backend -->
        <div class="glass-card p-3 small d-flex flex-column gap-2">
          <div class="fw-bold text-info">
            <i class="bi bi-shield-check me-1"></i>Endpoints Activos
          </div>
          <a
            href="http://localhost:8080/swagger-ui"
            target="_blank"
            class="text-decoration-none text-light d-flex justify-content-between align-items-center">
            <span><i class="bi bi-journal-code me-1 text-info"></i>Rust Swagger UI</span>
            <i class="bi bi-box-arrow-up-right text-secondary"></i>
          </a>
          <a
            href="http://localhost:8080/api/kpis"
            target="_blank"
            class="text-decoration-none text-light d-flex justify-content-between align-items-center">
            <span><i class="bi bi-filetype-json me-1 text-success"></i>GET /api/kpis</span>
            <i class="bi bi-box-arrow-up-right text-secondary"></i>
          </a>
          <a
            href="http://localhost:8081"
            target="_blank"
            class="text-decoration-none text-light d-flex justify-content-between align-items-center">
            <span><i class="bi bi-wind me-1 text-warning"></i>Airflow Web UI</span>
            <i class="bi bi-box-arrow-up-right text-secondary"></i>
          </a>
        </div>
      </aside>

      <!-- Contenido Principal del Dashboard Admin -->
      <div class="flex-grow-1 d-flex flex-column min-w-0">
        <!-- Topbar Bootstrap 5 Transparent Glass -->
        <header class="glass-navbar px-3 px-lg-4 py-3 d-flex flex-wrap justify-content-between align-items-center gap-3 sticky-top">
          <div>
            <h4 class="mb-0 fw-bold text-white d-flex align-items-center gap-2">
              <span>Last-Mile Command Center</span>
              <span class="badge rounded-pill badge-glass fw-normal small">Bootstrap 5 Glass</span>
            </h4>
            <small class="text-secondary">
              Rust Axum SSE · Apache Kafka (KRaft) · Apache Airflow Batch · Angular 21 Signals
            </small>
          </div>

          <div class="d-flex flex-wrap align-items-center gap-2">
            <!-- Estado del Stream SSE -->
            <div class="glass-card px-3 py-1 d-flex align-items-center gap-2 small">
              <span
                class="rounded-circle d-inline-block"
                style="width: 9px; height: 9px;"
                [style.background]="telemetry.sseConnected() ? '#10b981' : '#f59e0b'"
                [style.box-shadow]="telemetry.sseConnected() ? '0 0 8px #10b981' : 'none'"></span>
              <span [class.text-success]="telemetry.sseConnected()">
                {{ telemetry.sseConnected() ? 'SSE Stream En Vivo (:8080)' : 'Modo Demo / Reconectando SSE' }}
              </span>
            </div>

            <button
              class="btn btn-sm btn-glass"
              (click)="telemetry.loadDailyKpis()">
              <i class="bi bi-arrow-repeat me-1"></i>Refrescar KPIs
            </button>

            <button
              class="btn btn-sm"
              [class.btn-glass-amber]="telemetry.simulatorActive()"
              [class.btn-glass-cyan]="!telemetry.simulatorActive()"
              (click)="telemetry.triggerSimulation('toggle_auto')">
              <i
                class="bi me-1"
                [class.bi-pause-fill]="telemetry.simulatorActive()"
                [class.bi-play-fill]="!telemetry.simulatorActive()"></i>
              {{ telemetry.simulatorActive() ? 'Pausar Simulador' : 'Reanudar Simulador' }}
            </button>
          </div>
        </header>

        <!-- Cuerpo del Dashboard -->
        <main class="container-fluid p-3 p-lg-4 d-flex flex-column gap-4">
          <!-- Fila de 5 Tarjetas Resumen Ejecutivo (Flotilla + http://localhost:8080/api/kpis) -->
          <div class="row g-3">
            <div class="col-12 col-sm-6 col-xl">
              <div class="glass-card p-3 h-100 d-flex justify-content-between align-items-center">
                <div>
                  <div class="small text-secondary">Zonas Monitoreadas</div>
                  <div class="fs-3 fw-bold text-white font-mono">{{ telemetry.kpis().length }}</div>
                  <small class="text-info font-mono">GET /api/kpis</small>
                </div>
                <div class="rounded-3 p-3 bg-info bg-opacity-10 text-info fs-4">
                  <i class="bi bi-globe-americas"></i>
                </div>
              </div>
            </div>

            <div class="col-12 col-sm-6 col-xl">
              <div class="glass-card p-3 h-100 d-flex justify-content-between align-items-center">
                <div>
                  <div class="small text-secondary">Entregas Exitosas (KPI)</div>
                  <div class="fs-3 fw-bold text-white font-mono">
                    {{ telemetry.totalKpiSuccessful() }}
                    <span class="fs-6 text-secondary">/ {{ telemetry.totalKpiDeliveries() }}</span>
                  </div>
                  <small class="text-success font-mono">
                    Puntualidad: {{ telemetry.avgKpiOnTimeRate() }}%
                  </small>
                </div>
                <div class="rounded-3 p-3 bg-success bg-opacity-10 text-success fs-4">
                  <i class="bi bi-box-seam-fill"></i>
                </div>
              </div>
            </div>

            <div class="col-12 col-sm-6 col-xl">
              <div class="glass-card p-3 h-100 d-flex justify-content-between align-items-center">
                <div>
                  <div class="small text-secondary">Liquidación Total Zonas</div>
                  <div class="fs-3 fw-bold text-success font-mono">
                    \${{ telemetry.totalKpiPayoutUsd() | number:'1.2-2' }}
                  </div>
                  <small class="text-secondary">Consolidado diario Airflow</small>
                </div>
                <div class="rounded-3 p-3 bg-success bg-opacity-10 text-success fs-4">
                  <i class="bi bi-cash-stack"></i>
                </div>
              </div>
            </div>

            <div class="col-12 col-sm-6 col-xl">
              <div class="glass-card p-3 h-100 d-flex justify-content-between align-items-center">
                <div>
                  <div class="small text-secondary">Unidades GPS en Vivo</div>
                  <div class="fs-3 fw-bold text-white font-mono">
                    {{ telemetry.activeCount() }}
                  </div>
                  <small class="text-info font-mono">
                    Vel. Prom: {{ telemetry.avgSpeedKmh() }} km/h
                  </small>
                </div>
                <div class="rounded-3 p-3 bg-primary bg-opacity-10 text-primary fs-4">
                  <i class="bi bi-truck"></i>
                </div>
              </div>
            </div>

            <div class="col-12 col-sm-12 col-xl">
              <div
                class="glass-card p-3 h-100 d-flex justify-content-between align-items-center"
                [class.border-danger]="telemetry.offRouteCount() > 0 || telemetry.stoppedCount() > 0">
                <div>
                  <div class="small text-secondary">Desvíos (&gt;500m) / Paradas</div>
                  <div class="fs-3 fw-bold text-warning font-mono">
                    {{ telemetry.offRouteCount() }}
                    <span class="text-secondary">/</span>
                    <span class="text-danger">{{ telemetry.stoppedCount() }}</span>
                  </div>
                  <small class="text-warning font-mono">
                    {{ telemetry.alerts().length }} alertas emitidas
                  </small>
                </div>
                <div class="rounded-3 p-3 bg-danger bg-opacity-10 text-danger fs-4">
                  <i class="bi bi-shield-fill-exclamation"></i>
                </div>
              </div>
            </div>
          </div>

          <!-- Vista Principal: Mapa de KPIs y Flotilla + Feed Reactivo de Alertas -->
          @if (activeSection() === 'overview' || activeSection() === 'alerts') {
            <div class="row g-4">
              <div class="col-12 col-xxl-8">
                <app-live-map />
              </div>
              <div class="col-12 col-xxl-4">
                <app-alerts-feed />
              </div>
            </div>
          }

          <!-- Monitor de Procesos Batch Airflow y Tabla Completa de /api/kpis -->
          @if (activeSection() === 'overview' || activeSection() === 'batch') {
            <app-batch-monitor />
          }
        </main>
      </div>
    </div>
  `,
  styles: [
    `
      .cursor-pointer {
        cursor: pointer;
      }
    `,
  ],
})
export class AppComponent {
  readonly telemetry = inject(TelemetryService);
  readonly activeSection = signal<'overview' | 'alerts' | 'batch'>('overview');

  toggleZoneFilter(zone: string): void {
    const current = this.telemetry.selectedZoneFilter();
    this.telemetry.selectedZoneFilter.set(current === zone ? null : zone);
  }
}
