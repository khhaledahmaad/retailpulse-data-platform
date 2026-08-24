FROM apache/airflow:3.3.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    psycopg[binary] \
    pyarrow \
    python-dotenv \
    dbt-postgres