{{ config(materialized='view') }}

select
    raw_record:dispute_id::string as dispute_id,
    raw_record:claim_id::string as claim_id,
    raw_record:transaction_id::string as transaction_id,
    raw_record:dispute_reason::string as dispute_reason,
    raw_record:dispute_amount::number(12,2) as dispute_amount,
    raw_record:dispute_status::string as dispute_status,
    raw_record:opened_date::timestamp as opened_date,
    raw_record:card_network::string as card_network,
    loaded_at
from {{ source('fraud_raw', 'RAW_DISPUTES') }}
