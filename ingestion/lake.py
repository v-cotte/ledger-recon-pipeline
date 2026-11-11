"""Write datasets to the lake as Hive-partitioned Parquet.

Layout mirrors what lands in S3 in production:

    <lake>/bronze/<dataset>/dt=YYYY-MM-DD/part-0.parquet

Each run replaces its whole partition, so re-running or backfilling a day is
idempotent: you never get duplicated rows from a retry.
"""

from __future__ import annotations

import shutil
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MONEY = pa.decimal128(18, 2)
QTY = pa.decimal128(18, 8)
PRICE = pa.decimal128(18, 1)
TS = pa.timestamp("us", tz="UTC")

SCHEMAS: dict[str, pa.Schema] = {
    "exchange_trades": pa.schema([
        ("trade_id", pa.string()), ("pair", pa.string()), ("side", pa.string()),
        ("price_usd", PRICE), ("quantity_btc", QTY), ("executed_at", TS),
    ]),
    "ledger_entries": pa.schema([
        ("entry_id", pa.string()), ("trade_ref", pa.string()), ("account_id", pa.string()),
        ("side", pa.string()), ("quantity_btc", QTY), ("price_usd", PRICE),
        ("notional_usd", MONEY), ("fee_usd", MONEY), ("booked_at", TS),
    ]),
    "provider_statement": pa.schema([
        ("statement_line_id", pa.string()), ("trade_ref", pa.string()), ("side", pa.string()),
        ("settled_quantity_btc", QTY), ("settled_amount_usd", MONEY), ("fee_usd", MONEY),
        ("settlement_date", pa.date32()),
    ]),
    "planted_issues": pa.schema([("trade_ref", pa.string()), ("issue_type", pa.string())]),
}


def write_partition(lake_dir: Path, dataset: str, day: date, records: list) -> Path:
    schema = SCHEMAS[dataset].append(pa.field("_ingested_at", TS))
    ingested_at = datetime.now(timezone.utc)
    rows = [{**asdict(r), "_ingested_at": ingested_at} for r in records]
    table = pa.Table.from_pylist(rows, schema=schema)

    layer = "_ground_truth" if dataset == "planted_issues" else "bronze"
    partition = lake_dir / layer / dataset / f"dt={day.isoformat()}"
    if partition.exists():
        shutil.rmtree(partition)
    partition.mkdir(parents=True)

    path = partition / "part-0.parquet"
    pq.write_table(table, path)
    return path
