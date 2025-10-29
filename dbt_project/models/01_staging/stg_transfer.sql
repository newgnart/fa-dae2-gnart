with source as (
    select * from {{ source('raw_data', 'raw_transfer') }}
),

casted as (
    select
        id,
        block_number::bigint as block_number,
        to_timestamp(timestamp::bigint) AT TIME ZONE 'UTC' as block_timestamp,
        contract_address::varchar(42) as contract_address,
        "from"::varchar(42) as from_address,
        "to"::varchar(42) as to_address,
        value::numeric(38, 0) as amount_raw,
        _dlt_load_id,
        _dlt_id
    from source
)

select * from casted
