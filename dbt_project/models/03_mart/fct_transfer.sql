{{
    config(
        materialized='incremental',
        unique_key=['transaction_hash', 'log_index'],
        on_schema_change='fail',
        incremental_strategy='delete+insert'
    )
}}

WITH stg_transfer AS (
    SELECT * FROM {{ ref('stg_transfer') }}
    {% if is_incremental() %}
    -- Only process new blocks since last run
        WHERE block_number >= (SELECT COALESCE(MAX(block_number), 0) FROM {{ this }})
    {% endif %}
),

dim_stablecoin AS (
    SELECT * FROM {{ ref('dim_stablecoin') }}
    WHERE is_current = true  -- Only use current stablecoin metadata
),

parsed AS (
    SELECT
        -- Parse natural key from id (format: "0xtxhash_logindex")
        SPLIT_PART(id, '_', 2)::INTEGER AS log_index,
        TO_CHAR(block_timestamp, 'YYYYMMDD')::INTEGER AS date_key,

        -- Time dimension
        block_number,
        block_timestamp,
        contract_address,

        -- Contract/token dimension
        'ethereum' AS chain,
        from_address,  -- TODO: get chain from raw data when available

        -- Address dimensions
        to_address,
        SPLIT_PART(id, '_', 1) AS transaction_hash

    FROM stg_transfer
),

enriched AS (
    SELECT
        -- Keys
        p.transaction_hash,
        p.log_index,

        -- Time dimension
        p.date_key,
        p.block_number,
        p.block_timestamp,

        -- Contract/token dimension
        p.contract_address,
        p.chain,

        -- Address dimensions
        p.from_address,
        p.to_address,

        -- Join stablecoin metadata
        d.symbol,
        d.name,
        COALESCE(d.decimals, 18) AS decimals,

        -- Determine transaction type
        CASE
            WHEN p.from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
            WHEN p.to_address = '0x0000000000000000000000000000000000000000' THEN 'burn'
            ELSE 'transfer'
        END AS transaction_type,

        -- Convert to decimal amount using actual decimals from dim_stablecoin
        -- For stablecoins, amount ≈ USD value. TODO: have dim_price table
        {{ convert_token_amount('s.amount_raw', 'COALESCE(d.decimals, 18)', 2) }} AS amount

    FROM parsed AS p
    LEFT JOIN stg_transfer AS s
        ON
            p.transaction_hash = SPLIT_PART(s.id, '_', 1)
            AND p.log_index = SPLIT_PART(s.id, '_', 2)::INTEGER
    LEFT JOIN dim_stablecoin AS d
        ON
            LOWER(p.contract_address) = LOWER(d.contract_address)
            AND p.chain = d.chain
)

SELECT
    transaction_hash,
    log_index,
    date_key,
    block_number,
    block_timestamp,
    contract_address,
    chain,
    from_address,
    to_address,
    symbol,
    name,
    decimals,
    transaction_type,
    amount
FROM enriched
