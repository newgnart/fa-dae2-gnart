with source as (
    select * from {{ source('raw_data', 'raw_stablecoin') }}
),

casted as (
    select
        contract_address::varchar(42) as contract_address,
        chain::varchar(20) as chain,
        symbol::varchar(20) as symbol,
        name::varchar(100) as name,
        currency::varchar(10) as currency,
        backing_type::varchar(20) as backing_type,
        decimals::integer as decimals
    from source
)

select * from casted
