FROM apache/airflow:2.10.2-python3.11

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-http \
    psycopg2-binary \
    requests

COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root scripts /opt/airflow/scripts
