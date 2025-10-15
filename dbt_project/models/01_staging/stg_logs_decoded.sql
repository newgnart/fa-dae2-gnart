{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['contract_address', 'transaction_hash', 'index'], 'type': 'btree'},
        ]
    )
}}

select
    to_timestamp(time_stamp)::timestamp at time zone 'UTC' as block_time,
    block_number::bigint as block_number,
    address::varchar(42) as contract_address,
    transaction_hash::varchar(66) as transaction_hash,
    log_index::integer as index,
    topics::json->>0::varchar(66) as topic0,
    case when json_array_length(topics::json) >= 2 then topics::json->>1 end::varchar(66) as topic1,
    case when json_array_length(topics::json) >= 3 then topics::json->>2 end::varchar(66) as topic2,
    case when json_array_length(topics::json) >= 4 then topics::json->>3 end::varchar(66) as topic3,
    data::text as data
from {{ source('etherscan_raw', 'logs') }}
