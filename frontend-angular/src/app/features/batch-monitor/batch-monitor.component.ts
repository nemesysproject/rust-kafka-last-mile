import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AirflowService } from '../../core/services/airflow.service';
import { TelemetryService } from '../../core/services/telemetry.service';

@Component({
  selector: 'app-batch-monitor',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="row g-3">
      <!-- Columna 1: Orquestador Batch Apache Airflow -->
      <div class="col-12 col-xl-5">
        <div class="glass-panel p-3 p-lg-4 h-100 d-flex flex-column gap-3">
          <div class="d-flex justify-content-between align-items-start flex-wrap gap-2">
            <div>
              <span class="badge rounded-pill text-bg-primary bg-opacity-25 text-info border border-info border-opacity-50">
                <i class="bi bi-cpu-fill me-1"></i>ORQUESTADOR BATCH · APACHE AIRFLOW
              </span>
              <h5 class="mb-0 mt-1 fw-bold text-white">Control de DAGs y Cierre Operativo</h5>
            </div>
            <span
              class="badge rounded-pill"
              [class.text-bg-success]="airflow.airflowOnline()"
              [class.badge-glass]="!airflow.airflowOnline()">
              <i class="bi bi-shield-lock-fill me-1"></i>
              {{ airflow.airflowOnline() ? 'Airflow Proxy Activo' : 'Proxy Ready (Modo Demo)' }}
            </span>
          </div>

          @if (airflow.lastActionMessage()) {
            <div class="alert alert-info bg-info bg-opacity-10 border-info border-opacity-25 text-info py-2 px-3 small mb-0">
              <i class="bi bi-terminal me-1"></i>{{ airflow.lastActionMessage() }}
            </div>
          }

          <div class="d-flex flex-column gap-3">
            @for (dag of airflow.dags(); track dag.dag_id) {
              <div class="glass-card p-3 d-flex flex-column flex-sm-row justify-content-between align-items-sm-center gap-3">
                <div>
                  <div class="d-flex align-items-center flex-wrap gap-2">
                    <strong class="font-mono text-white">{{ dag.dag_id }}</strong>
                    <span class="badge badge-glass font-mono">{{ dag.schedule_interval }}</span>
                    <span
                      class="badge rounded-pill"
                      [class.text-bg-success]="dag.last_run_state === 'success'"
                      [class.text-bg-info]="dag.last_run_state === 'running'">
                      {{ dag.last_run_state === 'running' ? 'RUNNING...' : 'SUCCESS' }}
                    </span>
                  </div>
                  <p class="small text-secondary mb-2 mt-1">{{ dag.description }}</p>
                  <div class="d-flex flex-wrap gap-1">
                    @for (tag of dag.tags; track tag) {
                      <span class="badge bg-dark bg-opacity-50 text-secondary border border-secondary border-opacity-25">
                        #{{ tag }}
                      </span>
                    }
                  </div>
                </div>

                <button
                  class="btn btn-sm btn-glass-cyan px-3 text-nowrap"
                  [disabled]="dag.last_run_state === 'running'"
                  (click)="triggerBatchAndRefresh(dag.dag_id)">
                  <i class="bi bi-play-fill me-1"></i>
                  {{ dag.last_run_state === 'running' ? 'Ejecutando...' : 'Ejecutar DAG' }}
                </button>
              </div>
            }
          </div>
        </div>
      </div>

      <!-- Columna 2: Tabla Consolidada de http://localhost:8080/api/kpis -->
      <div class="col-12 col-xl-7">
        <div class="glass-panel p-3 p-lg-4 h-100 d-flex flex-column gap-3">
          <div class="d-flex justify-content-between align-items-center flex-wrap gap-2">
            <div>
              <span class="badge rounded-pill text-bg-success bg-opacity-25 text-success border border-success border-opacity-50">
                <i class="bi bi-bar-chart-fill me-1"></i>ENDPOINT: http://localhost:8080/api/kpis
              </span>
              <h5 class="mb-0 mt-1 fw-bold text-white">
                Rendimiento y Liquidación Diaria por Zona Monitoreada
              </h5>
            </div>

            <button
              class="btn btn-sm btn-glass"
              (click)="telemetry.loadDailyKpis()">
              <i class="bi bi-arrow-clockwise me-1"></i>Sincronizar /api/kpis
            </button>
          </div>

          <div class="table-responsive">
            <table class="table table-glass table-hover align-middle small">
              <thead>
                <tr>
                  <th>Zona Monitoreada</th>
                  <th>Fecha Reporte</th>
                  <th>Entregas (Exitosas / Total)</th>
                  <th>Tasa Puntualidad</th>
                  <th>Alertas Ruta</th>
                  <th class="text-end">Liquidación (USD)</th>
                </tr>
              </thead>
              <tbody>
                @for (kpi of telemetry.kpis(); track kpi.zone) {
                  <tr>
                    <td>
                      <div class="fw-bold text-white">
                        <i class="bi bi-geo-alt text-info me-1"></i>{{ kpi.zone }}
                      </div>
                    </td>
                    <td class="font-mono text-secondary">{{ kpi.report_date }}</td>
                    <td class="font-mono">
                      <span class="fw-bold text-white">{{ kpi.successful_deliveries }}</span>
                      <span class="text-secondary"> / {{ kpi.total_deliveries }}</span>
                    </td>
                    <td style="min-width: 140px;">
                      <div class="d-flex align-items-center gap-2">
                        <span
                          class="badge rounded-pill font-mono"
                          [class.text-bg-success]="kpi.on_time_rate >= 95"
                          [class.text-bg-warning]="kpi.on_time_rate < 95">
                          {{ kpi.on_time_rate | number:'1.1-1' }}%
                        </span>
                        <div class="progress flex-grow-1" style="height: 5px; background: rgba(255,255,255,0.1);">
                          <div
                            class="progress-bar"
                            [class.bg-success]="kpi.on_time_rate >= 95"
                            [class.bg-warning]="kpi.on_time_rate < 95"
                            [style.width.%]="kpi.on_time_rate"></div>
                        </div>
                      </div>
                    </td>
                    <td class="font-mono">
                      <span
                        class="badge rounded-pill"
                        [class.badge-glass]="kpi.total_alerts === 0"
                        [class.text-bg-warning]="kpi.total_alerts > 0">
                        {{ kpi.total_alerts }}
                      </span>
                    </td>
                    <td class="text-end font-mono fw-bold text-success">
                      \${{ kpi.driver_payout_usd | number:'1.2-2' }}
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `,
})
export class BatchMonitorComponent {
  readonly airflow = inject(AirflowService);
  readonly telemetry = inject(TelemetryService);

  triggerBatchAndRefresh(dagId: string): void {
    this.airflow.triggerDagRun(dagId);
    setTimeout(() => this.telemetry.loadDailyKpis(), 2200);
  }
}
