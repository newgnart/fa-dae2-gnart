-- SCD Type 2 dimension built on dbt snapshot
-- This model adds custom SCD2 column names while leveraging dbt's snapshot functionality

with snap_stablecoin as (
    select * from {{ ref('snap_stablecoin') }}
),

final as (
    select
        -- Business keys
        contract_address,
        chain,

        -- Attributes (tracked for changes)
        symbol,
        name,
        currency,
        backing_type,
        decimals,

        -- SCD Type 2 columns (mapped from dbt snapshot columns)
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current,
        dbt_updated_at as created_at

    from snap_stablecoin
)

select * from final
