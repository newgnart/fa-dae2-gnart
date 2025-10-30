with source as (
    select * from {{ source('raw_data', 'raw_transfer') }}
),

casted as (
    select
        id,
        block_number::bigint as block_number,
        {% if target.type == 'postgres' %}
        to_timestamp(timestamp::bigint) AT TIME ZONE 'UTC' as block_timestamp,
        {% else %}
        to_timestamp(timestamp::bigint) as block_timestamp,
        {% endif %}
        contract_address::varchar(42) as contract_address,
        {% if target.type == 'postgres' %}
        "from"::varchar(42) as from_address,
        "to"::varchar(42) as to_address,
        {% else %}
        "FROM"::varchar(42) as from_address,
        "TO"::varchar(42) as to_address,
        {% endif %}
        value::numeric(38, 0) as amount_raw,
        _dlt_load_id,
        _dlt_id
    from source
)

select * from casted
