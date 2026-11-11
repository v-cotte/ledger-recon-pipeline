"""Ingest one day (or a range of days) into the bronze layer.

    python -m ingestion.run --date 2026-11-09 --source kraken
    python -m ingestion.run --start 2026-11-01 --end 2026-11-07 --source synthetic
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

from ingestion.lake import write_partition
from ingestion.simulate import simulate
from ingestion.sources import KrakenSource, SyntheticSource

log = logging.getLogger("ingestion")


def ingest_day(day: date, source: str, lake_dir: Path, max_trades: int = 300) -> dict[str, int]:
    src = KrakenSource(max_trades=max_trades) if source == "kraken" else SyntheticSource(max_trades)
    trades = src.fetch(day)
    if not trades:
        raise RuntimeError(f"No trades returned for {day} from {source}")

    ledger, statement, issues = simulate(day, trades)
    counts = {}
    for dataset, records in [
        ("exchange_trades", trades),
        ("ledger_entries", ledger),
        ("provider_statement", statement),
        ("planted_issues", issues),
    ]:
        write_partition(lake_dir, dataset, day, records)
        counts[dataset] = len(records)

    log.info("Ingested %s from %s: %s", day, source, counts)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat, help="inclusive")
    parser.add_argument("--source", choices=["kraken", "synthetic"], default="synthetic")
    parser.add_argument("--max-trades", type=int, default=300)
    parser.add_argument("--lake-dir", type=Path, default=Path(os.getenv("LAKE_DIR", "data/lake")))
    args = parser.parse_args()

    if args.date:
        days = [args.date]
    elif args.start and args.end:
        days = [args.start + timedelta(d) for d in range((args.end - args.start).days + 1)]
    else:
        parser.error("pass --date or --start/--end")

    for day in days:
        ingest_day(day, args.source, args.lake_dir, args.max_trades)


if __name__ == "__main__":
    main()
