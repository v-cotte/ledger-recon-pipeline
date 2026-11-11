-- One row per ledger entry. Duplicates are NOT removed here on purpose:
-- a trade booked twice is a break the reconciliation has to surface.

select
    entry_id,
    trade_ref,
    account_id,
    side,
    quantity_btc,
    cast(price_usd as decimal(18, 2))   as price_usd,
    notional_usd,
    fee_usd,
    booked_at,
    partition_date                      as trade_date,
    count(*) over (partition by trade_ref) as entries_for_trade
from {{ ref('brz_ledger_entries') }}
