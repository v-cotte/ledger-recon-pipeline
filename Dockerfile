FROM apache/airflow:3.3.2-python3.12

# Ingestion deps go in Airflow's environment (they run inside tasks)
RUN pip install --no-cache-dir "pyarrow>=17" "requests>=2.32" "duckdb>=1.1"

# dbt gets its own virtualenv so its dependencies never clash with Airflow's.
# It lives in the airflow user's home: the image runs as a non-root user that
# can't write to /opt.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb==1.11.1"
