with stg_stablecoin as (
    select * from {{ ref('stg_stablecoin') }}
),

enriched as (
    select
        contract_address,
        chain,
        symbol,
        name,
        currency,
        backing_type,
        decimals,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from stg_stablecoin
)

select * from enriched
