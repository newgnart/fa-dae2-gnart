-- Test that block numbers are within expected range

select
    block_number
from {{ ref('stg_logs_decoded') }}
where
    block_number < 0
    or block_number > 999999999  -- reasonable upper bound
