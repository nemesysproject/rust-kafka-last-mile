"""
DAG: daily_fleet_kpi
Cierre Diario (Batch): Cada 24 horas (o bajo demanda desde el dashboard Angular 21),
consolida las entregas exitosas en PostgreSQL, calcula bonos/liquidación de repartidores,
genera reporte de rendimiento por zona y limpia datos históricos.
"""

import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/scripts")
from data_aggregator import compute_daily_zone_kpis, purge_historical_telemetry


default_args = {
    "owner": "swiftroute-ops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_consolidate_kpis(**context):
    logical_date = context.get("ds")
    summary = compute_daily_zone_kpis(logical_date)
    print(f"[SwiftRoute Batch] KPIs consolidados para {len(summary)} zonas: {summary}")
    return summary


def task_cleanup_historical_data():
    purged = purge_historical_telemetry(retention_days=7)
    print(f"[SwiftRoute Batch] Registros históricos de telemetría depurados: {purged}")
    return purged


with DAG(
    dag_id="daily_fleet_kpi",
    default_args=default_args,
    description="Cálculo diario de puntualidad, liquidación de bonos y limpieza de telemetría histórica",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["swiftroute", "kpi", "batch", "last-mile"],
) as dag:

    consolidate_zone_kpis = PythonOperator(
        task_id="consolidate_zone_kpis_and_payouts",
        python_callable=task_consolidate_kpis,
    )

    cleanup_old_telemetry = PythonOperator(
        task_id="cleanup_historical_gps_telemetry",
        python_callable=task_cleanup_historical_data,
    )

    consolidate_zone_kpis >> cleanup_old_telemetry
