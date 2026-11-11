-- The daily report must add up to the trade-level detail.

with detail as (
    select trade_date, count(*) as trades, count(*) filter (where is_break) as breaks
    from {{ ref('fct_trade_reconciliation') }}
    group by trade_date
)

select r.trade_date, r.trades, d.trades as detail_trades, r.breaks, d.breaks as detail_breaks
from {{ ref('rpt_daily_reconciliation') }} r
join detail d using (trade_date)
where r.trades <> d.trades or r.breaks <> d.breaks
   or r.matched + r.breaks <> r.trades
