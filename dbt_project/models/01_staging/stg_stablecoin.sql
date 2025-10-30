WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_stablecoin') }}
),

casted AS (
    SELECT
        contract_address::VARCHAR(42) AS contract_address,
        chain::VARCHAR(20) AS chain,
        symbol::VARCHAR(20) AS symbol,
        name::VARCHAR(100) AS name,
        currency::VARCHAR(10) AS currency,
        backing_type::VARCHAR(20) AS backing_type,
        decimals::INTEGER AS decimals
    FROM source
)

SELECT * FROM casted
