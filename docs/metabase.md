# Reconciliation dashboard in Metabase

After every successful `dbt build`, Airflow's `publish_gold` task copies the gold tables to the `serving-db` Postgres. Metabase reads from there.

## 1. First-time setup

1. `docker compose up --build`, then open http://localhost:3000.
2. Create the admin user (any email / password; it stays local).
3. When asked to add data, choose **PostgreSQL**:

| Field | Value |
|---|---|
| Display name | Reconciliation |
| Host | `serving-db` |
| Port | `5432` |
| Database name | `serving` |
| Username / Password | `recon` / `recon` |

> Use `serving-db`, not `localhost`: Metabase runs in its own container and reaches Postgres by its service name.

If the tables don't show up yet, trigger `ingest_trades` in Airflow first (the transform DAG runs and publishes automatically), then in Metabase go to **Admin → Databases → Reconciliation → Sync database schema now**.

## 2. Questions

Create each with **New → SQL query**, pick the Reconciliation database, run it, set the visualization, and save.

**Match rate over time** (Line chart)

```sql
select trade_date, match_rate_pct
from gold.rpt_daily_reconciliation
order by trade_date
```

**Breaks by type** (Bar chart, stacked: Display → Stacking → Stack)

```sql
select trade_date, recon_status, count(*) as breaks
from gold.fct_trade_reconciliation
where is_break
group by 1, 2
order by 1
```

**USD exposure per day** (Bar chart)

```sql
select trade_date, exposure_usd
from gold.rpt_daily_reconciliation
order by trade_date
```

**Open breaks to review** (Table)

```sql
select trade_id, trade_date, recon_status, exposure_usd,
       ledger_notional_usd, statement_amount_usd, amount_diff_usd, fee_diff_usd
from gold.fct_trade_reconciliation
where is_break
order by trade_date desc, exposure_usd desc
```

**Latest match rate** (Number)

```sql
select match_rate_pct
from gold.rpt_daily_reconciliation
order by trade_date desc
limit 1
```

## 3. Dashboard

**New → Dashboard → "Daily reconciliation"**, add the five questions: the number on top, the three charts in the middle, the open-breaks table at the bottom.

To see it react, simulate a bad day and re-run the pipeline:

```bash
python -m ingestion.run --date 2026-11-10 --source synthetic --max-trades 120 --break-rate 0.06
```

then trigger `transform_reconciliation` in Airflow and refresh the dashboard: the match rate drops and the breaks bar spikes.
