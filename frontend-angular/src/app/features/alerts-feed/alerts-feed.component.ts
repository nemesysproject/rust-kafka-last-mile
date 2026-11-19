import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TelemetryService } from '../../core/services/telemetry.service';

@Component({
  selector: 'app-alerts-feed',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="glass-panel p-3 p-lg-4 h-100 d-flex flex-column gap-3">
      <!-- Header -->
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <span class="badge rounded-pill text-bg-warning bg-opacity-25 text-warning border border-warning border-opacity-50">
            <i class="bi bi-bell-fill me-1"></i>TÓPICO KAFKA: route-alerts
          </span>
          <h5 class="mb-0 mt-1 fw-bold text-white">Feed Reactivo de Incidencias</h5>
        </div>
        @if (telemetry.alerts().length > 0) {
          <button
            class="btn btn-sm btn-glass"
            (click)="telemetry.clearAlerts()">
            <i class="bi bi-trash3 me-1"></i>Limpiar
          </button>
        }
      </div>

      <!-- Reglas de Negocio Activas -->
      <div class="d-flex flex-column gap-2">
        <div class="glass-card p-2 px-3 small border-start border-3 border-info">
          <i class="bi bi-shield-exclamation text-info me-1"></i>
          <strong>Regla 1 (Geocerca):</strong> Alerta si desvío &gt; <code>500 m</code> de ruta asignada
        </div>
        <div class="glass-card p-2 px-3 small border-start border-3 border-danger">
          <i class="bi bi-stopwatch text-danger me-1"></i>
          <strong>Regla 2 (Inactividad):</strong> Alerta si <code>0 km/h</code> por &gt; <code>10 min</code> fuera de destino
        </div>
      </div>

      <!-- Lista de Alertas con Scroll -->
      <div class="d-flex flex-column gap-2 overflow-auto pe-1" style="max-height: 540px;">
        @for (alert of telemetry.alerts(); track alert.alert_id) {
          <div
            class="glass-card p-3 border-start border-4"
            [class.border-danger]="alert.severity === 'CRITICAL'"
            [class.border-warning]="alert.severity === 'WARNING'">
            <div class="d-flex justify-content-between align-items-center mb-1">
              <span
                class="badge rounded-pill"
                [class.text-bg-danger]="alert.severity === 'CRITICAL'"
                [class.text-bg-warning]="alert.severity === 'WARNING'">
                <i
                  class="bi me-1"
                  [class.bi-sign-stop-fill]="alert.alert_type !== 'ROUTE_DEVIATION'"
                  [class.bi-compass-fill]="alert.alert_type === 'ROUTE_DEVIATION'"></i>
                {{ alert.alert_type === 'ROUTE_DEVIATION' ? 'DESVÍO DE RUTA' : 'PARADA NO AUTORIZADA' }}
              </span>
              <span class="small text-secondary font-mono">
                {{ alert.created_at | date:'HH:mm:ss' }}
              </span>
            </div>

            <p class="small text-light mb-2">{{ alert.message }}</p>

            <div class="d-flex justify-content-between align-items-center small text-secondary border-top border-light border-opacity-10 pt-2">
              <span>
                <strong class="text-white">{{ alert.driver_id }}</strong> · {{ alert.zone }}
              </span>
              <span class="badge badge-glass font-mono">
                {{ alert.alert_type === 'ROUTE_DEVIATION'
                  ? (alert.metric_value | number:'1.0-0') + ' m'
                  : (alert.metric_value | number:'1.1-1') + ' min' }}
              </span>
            </div>
          </div>
        } @empty {
          <div class="glass-card p-4 text-center my-auto">
            <i class="bi bi-check2-circle fs-2 text-success d-block mb-2"></i>
            <div class="fw-semibold text-white">Sin alertas activas en cola</div>
            <p class="small text-secondary mb-0 mt-1">
              Usa los botones <strong>Desvío (&gt;500m)</strong> o <strong>Parada (&gt;10m)</strong> en el mapa para emitir alertas en vivo desde Rust/Kafka.
            </p>
          </div>
        }
      </div>
    </div>
  `,
})
export class AlertsFeedComponent {
  readonly telemetry = inject(TelemetryService);
}
