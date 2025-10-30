-- SCD Type 2 dimension built on dbt snapshot
-- This model adds custom SCD2 column names while leveraging dbt's snapshot functionality

WITH snap_stablecoin AS (
    SELECT * FROM {{ ref('snap_stablecoin') }}
),

final AS (
    SELECT
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
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to,
        dbt_updated_at AS created_at,
        COALESCE(dbt_valid_to IS NULL, FALSE) AS is_current

    FROM snap_stablecoin
)

SELECT * FROM final
