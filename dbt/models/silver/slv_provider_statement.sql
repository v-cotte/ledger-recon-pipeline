-- One row per statement line (re-delivered lines are deduplicated).

select
    statement_line_id,
    trade_ref,
    side,
    settled_quantity_btc,
    settled_amount_usd,
    fee_usd,
    settlement_date,
    partition_date as trade_date
from {{ ref('brz_provider_statement') }}
qualify row_number() over (partition by statement_line_id order by _ingested_at desc) = 1
