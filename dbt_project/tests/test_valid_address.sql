-- Test that all addresses are valid Ethereum addresses (42 chars with 0x prefix)

select
    contract_address,
    count(*) as invalid_count
from {{ ref('stg_logs_decoded') }}
where
    contract_address is null
    or length(contract_address) != 42
    or substring(contract_address, 1, 2) != '0x'
group by contract_address
having count(*) > 0
