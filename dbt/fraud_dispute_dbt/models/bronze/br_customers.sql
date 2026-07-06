{{ config(materialized='view') }}

select
    raw_record:customer_id::string as customer_id,
    raw_record:account_id::string as account_id,
    raw_record:customer_age::number as customer_age,
    raw_record:account_status::string as account_status,
    raw_record:state::string as state,
    raw_record:created_at::timestamp as created_at,
    loaded_at
from {{ source('fraud_raw', 'RAW_CUSTOMERS') }}
