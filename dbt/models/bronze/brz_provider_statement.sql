-- Bronze: raw records exactly as landed, plus the partition date.
select
    *,
    cast(dt as date) as partition_date
from {{ source('lake', 'provider_statement') }}
