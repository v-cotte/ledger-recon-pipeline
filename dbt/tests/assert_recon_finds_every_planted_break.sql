-- The simulator records every break it plants. The reconciliation must find
-- each one with the right type, and must not flag anything else.
-- Returns failing rows (missed, misclassified or false-positive breaks).

with expected as (
    select trade_ref as trade_id, issue_type as expected_status
    from {{ source('ground_truth', 'planted_issues') }}
),

detected as (
    select trade_id, recon_status as detected_status
    from {{ ref('fct_trade_reconciliation') }}
    where is_break
)

select
    coalesce(e.trade_id, d.trade_id) as trade_id,
    e.expected_status,
    d.detected_status
from expected e
full outer join detected d on d.trade_id = e.trade_id
where e.expected_status is distinct from d.detected_status
