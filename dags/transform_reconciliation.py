"""Bronze -> silver -> gold with dbt, then publish gold for BI and report breaks.

Asset-driven: runs whenever `ingest_trades` updates the `lake_bronze` asset,
instead of on a clock, so transformations never run on half-landed data.
`dbt build` runs models and tests together: if a test fails, the run fails
and nothing downstream is reported as reconciled.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, dag, task

PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/project")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
LAKE_DIR = Path(os.getenv("LAKE_DIR", "/opt/project/data/lake"))
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "/opt/project/data/warehouse.duckdb")
# Alert when a day has more than this many USD in open breaks
EXPOSURE_ALERT_USD = float(os.getenv("EXPOSURE_ALERT_USD", "25000"))
SERVING_DB_URL = os.getenv("SERVING_DB_URL")

LAKE_BRONZE = Asset(name="lake_bronze", uri=f"file://{LAKE_DIR}/bronze")
RECON_GOLD = Asset(name="recon_gold", uri=f"file://{WAREHOUSE_PATH}#gold")

log = logging.getLogger(__name__)


@dag(
    schedule=[LAKE_BRONZE],
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["dbt", "silver", "gold", "reconciliation"],
    doc_md=__doc__,
)
def transform_reconciliation():
    @task.bash(outlets=[RECON_GOLD])
    def dbt_build() -> str:
        return (
            f"cd {PROJECT_DIR} && "
            f"{DBT_BIN} deps --project-dir dbt --profiles-dir dbt && "
            f"{DBT_BIN} build --project-dir dbt --profiles-dir dbt"
        )

    @task
    def report_breaks() -> list[dict]:
        import duckdb

        with duckdb.connect(WAREHOUSE_PATH, read_only=True) as con:
            rows = con.sql(
                """
                select trade_date, trades, breaks, match_rate_pct, exposure_usd
                from gold.rpt_daily_reconciliation
                order by trade_date desc
                limit 7
                """
            ).fetchall()

        summary = []
        for trade_date, trades, breaks, match_rate, exposure in rows:
            summary.append({"trade_date": str(trade_date), "trades": trades, "breaks": breaks,
                            "match_rate_pct": match_rate, "exposure_usd": float(exposure)})
            line = f"{trade_date}: {breaks}/{trades} breaks, match rate {match_rate}%, exposure ${exposure:,.2f}"
            if exposure > EXPOSURE_ALERT_USD:
                # In production this posts to Slack / PagerDuty for the Accounting on-call
                log.warning("RECON ALERT %s", line)
            else:
                log.info(line)
        return summary

    @task
    def publish_gold() -> dict[str, int]:
        """Copy gold tables to the Postgres serving DB that Metabase reads."""
        from serving.publish import publish

        if not SERVING_DB_URL:
            log.info("SERVING_DB_URL not set; skipping publish")
            return {}
        return publish(WAREHOUSE_PATH, SERVING_DB_URL)

    built = dbt_build()
    built >> [publish_gold(), report_breaks()]


transform_reconciliation()
