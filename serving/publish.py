"""Publish gold tables from the DuckDB warehouse to Postgres for BI (Metabase).

Why a separate serving database: DuckDB allows a single writer, so a BI tool
holding the warehouse open would block `dbt build`. Gold is small and
read-heavy, so it is copied to Postgres after every successful build.

Each table is swapped atomically: the new version is loaded into a staging
table and renamed in one transaction, so dashboards never see a half-loaded
table.

    python -m serving.publish --pg-url postgresql://recon:recon@localhost:5432/serving
"""

from __future__ import annotations

import argparse
import logging
import os
import re

import duckdb
import psycopg
from psycopg import sql

log = logging.getLogger("serving")

GOLD_TABLES = ("fct_trade_reconciliation", "rpt_daily_reconciliation")


def _pg_type(duckdb_type: str) -> str:
    t = duckdb_type.upper()
    if m := re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", t):
        return f"numeric({m[1]},{m[2]})"
    return {
        "VARCHAR": "text",
        "BOOLEAN": "boolean",
        "DATE": "date",
        "TIMESTAMP WITH TIME ZONE": "timestamptz",
        "TIMESTAMP": "timestamp",
        "BIGINT": "bigint",
        "HUGEINT": "numeric(38,0)",
        "INTEGER": "integer",
        "DOUBLE": "double precision",
        "FLOAT": "real",
    }.get(t, "text")


def publish(warehouse_path: str, pg_url: str, tables: tuple[str, ...] = GOLD_TABLES,
            schema: str = "gold") -> dict[str, int]:
    counts: dict[str, int] = {}
    with duckdb.connect(warehouse_path, read_only=True) as duck, psycopg.connect(pg_url) as pg:
        pg.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(schema)))

        for table in tables:
            columns = duck.execute(f"describe gold.{table}").fetchall()  # (name, type, ...)
            rows = duck.execute(f"select * from gold.{table}").fetchall()
            staging = f"{table}__staging"

            col_defs = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(_pg_type(dtype)))
                for name, dtype, *_ in columns
            )
            col_names = sql.SQL(", ").join(sql.Identifier(name) for name, *_ in columns)

            with pg.transaction():
                pg.execute(sql.SQL("drop table if exists {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(staging)))
                pg.execute(sql.SQL("create table {}.{} ({})").format(
                    sql.Identifier(schema), sql.Identifier(staging), col_defs))
                with pg.cursor().copy(sql.SQL("copy {}.{} ({}) from stdin").format(
                        sql.Identifier(schema), sql.Identifier(staging), col_names)) as copy:
                    for row in rows:
                        copy.write_row(row)
                # atomic swap: readers see the old table until this commits
                pg.execute(sql.SQL("drop table if exists {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)))
                pg.execute(sql.SQL("alter table {}.{} rename to {}").format(
                    sql.Identifier(schema), sql.Identifier(staging), sql.Identifier(table)))

            counts[table] = len(rows)
            log.info("Published %s.%s: %d rows", schema, table, len(rows))
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse", default=os.getenv("WAREHOUSE_PATH", "data/warehouse.duckdb"))
    parser.add_argument("--pg-url", default=os.getenv("SERVING_DB_URL"), required=not os.getenv("SERVING_DB_URL"))
    args = parser.parse_args()
    publish(args.warehouse, args.pg_url)


if __name__ == "__main__":
    main()
