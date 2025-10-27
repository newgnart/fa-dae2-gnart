with stg_transfer as (
    select * from {{ ref('stg_transfer') }}
),

parsed as (
    select
        -- Parse natural key from id (format: "0xtxhash_logindex")
        split_part(id, '_', 1) as transaction_hash,
        split_part(id, '_', 2)::integer as log_index,

        -- Time dimension
        to_char(block_timestamp, 'YYYYMMDD')::integer as date_key,
        block_number,
        block_timestamp,

        -- Contract/token dimension
        contract_address,

        -- Address dimensions
        from_address,
        to_address,

        -- Metrics (amount_raw is in smallest unit, e.g., wei or token's smallest unit)
        amount_raw,

        -- Metadata
        id as source_id
    from stg_transfer
),

enriched as (
    select
        *,

        -- Business enrichment: Determine transaction type
        case
            when from_address = '0x0000000000000000000000000000000000000000' then 'mint'
            when to_address = '0x0000000000000000000000000000000000000000' then 'burn'
            else 'transfer'
        end as transaction_type,

        -- Convert to decimal amount (assuming 18 decimals for now)
        -- TODO: Join with dim_stablecoin to use actual decimals per token
        (amount_raw / 1000000000000000000.0)::decimal(28, 8) as amount,

        -- For stablecoins, amount ≈ USD value
        (amount_raw / 1000000000000000000.0)::decimal(28, 2) as usd_value

    from parsed
)

select * from enriched
