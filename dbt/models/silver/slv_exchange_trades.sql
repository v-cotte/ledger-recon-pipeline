{{
    config(
        materialized = 'incremental',
        unique_key = 'trade_id',
        incremental_strategy = 'delete+insert'
    )
}}

-- One row per exchange trade. Incremental: on each run only the latest
-- partitions are reprocessed, and re-running a day replaces it (idempotent).

with source as (

    select * from {{ ref('brz_exchange_trades') }}

    {% if is_incremental() %}
    where partition_date >= (select max(trade_date) from {{ this }})
    {% endif %}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by trade_id order by _ingested_at desc) = 1

)

select
    trade_id,
    pair,
    side,
    cast(price_usd as decimal(18, 2))                       as price_usd,
    quantity_btc,
    round(cast(price_usd * quantity_btc as decimal(38, 10)), 2) as notional_usd,
    executed_at,
    partition_date                                          as trade_date,  -- UTC trade date from the partition
    _ingested_at
from deduplicated
