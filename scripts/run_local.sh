#!/usr/bin/env bash
# Run the whole pipeline without Airflow: ingest a date range, then dbt build.
#   ./scripts/run_local.sh 2026-11-01 2026-11-07 [kraken|synthetic]
set -euo pipefail
START=${1:?start date}; END=${2:?end date}; SOURCE=${3:-synthetic}
cd "$(dirname "$0")/.."
mkdir -p data
python -m ingestion.run --start "$START" --end "$END" --source "$SOURCE"
dbt deps --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
