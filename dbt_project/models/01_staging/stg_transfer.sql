WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_transfer') }}
),

casted AS (
    SELECT
        id,
        block_number::BIGINT AS block_number,
        {% if target.type == 'postgres' %}
            TO_TIMESTAMP(timestamp::BIGINT) AT TIME ZONE 'UTC' AS block_timestamp,
        {% else %}
            TO_TIMESTAMP(timestamp::BIGINT) AS block_timestamp,
        {% endif %}
        contract_address::VARCHAR(42) AS contract_address,
        {% if target.type == 'postgres' %}
            "from"::VARCHAR(42) AS from_address,
            "to"::VARCHAR(42) AS to_address,
        {% else %}
            "FROM"::VARCHAR(42) AS from_address,
            "TO"::VARCHAR(42) AS to_address,
        {% endif %}
        value::NUMERIC(38, 0) AS amount_raw,
        _dlt_load_id,
        _dlt_id
    FROM source
)

SELECT * FROM casted
