"""Daily ingestion into the bronze layer.

Each run ingests its data interval: the full UTC day that just closed (the run
due at 00:00 on Nov 8 ingests Nov 7). Backfills work out of the box:

    # ingests Nov 1 -> Nov 7
    airflow backfill create --dag-id ingest_trades \
        --from-date 2026-11-02 --to-date 2026-11-08

Writes are idempotent (each run replaces its own partition), which makes
retries and re-runs safe. When the run succeeds it updates the `lake_bronze`
asset, which triggers the transformation DAG.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

LAKE_DIR = Path(os.getenv("LAKE_DIR", "/opt/project/data/lake"))
LAKE_BRONZE = Asset(name="lake_bronze", uri=f"file://{LAKE_DIR}/bronze")


@dag(
    # Interval timetable: the run at 00:00 UTC on day D+1 processes the full day D.
    # (Airflow 3's plain "@daily" gives zero-length intervals.)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,  # back off if the exchange API rate-limits us
    },
    tags=["ingestion", "bronze"],
    doc_md=__doc__,
)
def ingest_trades():
    @task(outlets=[LAKE_BRONZE])
    def ingest_day(data_interval_start=None) -> dict[str, int]:
        from ingestion.run import ingest_day as run_ingestion

        day = data_interval_start.date()
        source = os.getenv("INGEST_SOURCE", "kraken")
        return run_ingestion(day, source, LAKE_DIR)

    ingest_day()


ingest_trades()
