-- Guard against timezone bugs: a trade's date must be its UTC execution date.
select trade_id, executed_at, trade_date
from {{ ref('slv_exchange_trades') }}
where cast(timezone('UTC', executed_at) as date) <> trade_date
