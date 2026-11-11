-- One row per trade with its reconciliation status across three sources:
--   exchange (what happened) · ledger (what we booked) · statement (what settled)
-- Each break gets exactly one status, checked in order of severity.

{% set tol = var('amount_tolerance_usd') %}

with exchange as (

    select trade_id, trade_date, side, notional_usd
    from {{ ref('slv_exchange_trades') }}

),

ledger as (

    -- collapse to one row per trade; keep the entry count to catch duplicates
    select
        trade_ref,
        min(trade_date)        as trade_date,
        count(*)               as ledger_entries,
        min(notional_usd)      as ledger_notional_usd,
        min(fee_usd)           as ledger_fee_usd,
        sum(notional_usd)      as ledger_notional_total_usd
    from {{ ref('slv_ledger_entries') }}
    group by trade_ref

),

statement as (

    select
        trade_ref,
        trade_date,
        settled_amount_usd     as statement_amount_usd,
        fee_usd                as statement_fee_usd,
        settlement_date
    from {{ ref('slv_provider_statement') }}

),

joined as (

    select
        coalesce(e.trade_id, l.trade_ref, s.trade_ref)        as trade_id,
        coalesce(e.trade_date, l.trade_date, s.trade_date)    as trade_date,
        e.trade_id is not null                                as in_exchange,
        l.trade_ref is not null                               as in_ledger,
        s.trade_ref is not null                               as in_statement,
        coalesce(l.ledger_entries, 0)                         as ledger_entries,
        e.notional_usd                                        as exchange_notional_usd,
        l.ledger_notional_usd,
        s.statement_amount_usd,
        l.ledger_fee_usd,
        s.statement_fee_usd,
        l.ledger_notional_usd - s.statement_amount_usd        as amount_diff_usd,
        l.ledger_fee_usd - s.statement_fee_usd                as fee_diff_usd,
        s.settlement_date,
        datediff('day', coalesce(e.trade_date, l.trade_date, s.trade_date), s.settlement_date)
                                                              as settlement_lag_days,
        l.ledger_notional_total_usd
    from exchange e
    full outer join ledger l    on l.trade_ref = e.trade_id
    full outer join statement s on s.trade_ref = coalesce(e.trade_id, l.trade_ref)

),

classified as (

    select
        *,
        case
            when not in_exchange                                then 'missing_in_exchange'
            when not in_ledger                                  then 'missing_in_ledger'
            when ledger_entries > 1                             then 'duplicate_in_ledger'
            when not in_statement                               then 'missing_in_statement'
            when abs(amount_diff_usd) > {{ tol }}               then 'amount_mismatch'
            when abs(fee_diff_usd) > {{ tol }}                  then 'fee_mismatch'
            when settlement_lag_days > {{ var('max_settlement_lag_days') }} then 'late_settlement'
            else 'matched'
        end as recon_status
    from joined

)

select
    *,
    recon_status <> 'matched' as is_break,
    -- USD at stake for each break, used to prioritise follow-up
    case recon_status
        when 'matched'              then 0
        when 'missing_in_ledger'    then statement_amount_usd
        when 'missing_in_exchange'  then coalesce(ledger_notional_usd, statement_amount_usd)
        when 'duplicate_in_ledger'  then ledger_notional_total_usd - ledger_notional_usd
        when 'missing_in_statement' then ledger_notional_usd
        when 'amount_mismatch'      then abs(amount_diff_usd)
        when 'fee_mismatch'         then abs(fee_diff_usd)
        when 'late_settlement'      then statement_amount_usd
    end as exposure_usd
from classified
