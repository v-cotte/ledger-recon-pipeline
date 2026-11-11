-- Daily KPIs for Accounting / Treasury: how much reconciled and what is open.

select
    trade_date,
    count(*)                                                        as trades,
    count(*) filter (where not is_break)                            as matched,
    count(*) filter (where is_break)                                as breaks,
    round(100.0 * count(*) filter (where not is_break) / count(*), 2) as match_rate_pct,
    count(*) filter (where recon_status = 'missing_in_ledger')      as missing_in_ledger,
    count(*) filter (where recon_status = 'duplicate_in_ledger')    as duplicate_in_ledger,
    count(*) filter (where recon_status = 'missing_in_statement')   as missing_in_statement,
    count(*) filter (where recon_status = 'amount_mismatch')        as amount_mismatch,
    count(*) filter (where recon_status = 'fee_mismatch')           as fee_mismatch,
    count(*) filter (where recon_status = 'late_settlement')        as late_settlement,
    count(*) filter (where recon_status = 'missing_in_exchange')    as missing_in_exchange,
    round(sum(exposure_usd), 2)                                     as exposure_usd,
    round(sum(exchange_notional_usd), 2)                            as traded_notional_usd
from {{ ref('fct_trade_reconciliation') }}
group by trade_date
