"""
DAG: route_optimization
Optimización y asignación de rutas del día siguiente en función del desempeño por zona
y las incidencias de geocercas registradas en PostgreSQL.
"""

import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/scripts")
from data_aggregator import generate_next_day_route_optimizations


default_args = {
    "owner": "swiftroute-routing",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_optimize_next_day_routes():
    plans = generate_next_day_route_optimizations()
    print(f"[SwiftRoute Routing] Planes de ruta generados para {len(plans)} repartidores: {plans}")
    return plans


with DAG(
    dag_id="route_optimization",
    default_args=default_args,
    description="Asignación y optimización de rutas de entrega para el día siguiente",
    schedule="0 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["swiftroute", "routing", "optimization"],
) as dag:

    optimize_routes = PythonOperator(
        task_id="compute_next_day_optimal_routes",
        python_callable=task_optimize_next_day_routes,
    )
