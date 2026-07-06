{{ config(materialized='view') }}

select
    raw_record:transaction_id::string as transaction_id,
    raw_record:customer_id::string as customer_id,
    raw_record:account_id::string as account_id,
    raw_record:merchant_id::string as merchant_id,
    raw_record:transaction_amount::number(12,2) as transaction_amount,
    raw_record:transaction_timestamp::timestamp as transaction_timestamp,
    raw_record:transaction_status::string as transaction_status,
    raw_record:card_network::string as card_network,
    raw_record:merchant_category::string as merchant_category,
    raw_record:country::string as country,
    loaded_at
from {{ source('fraud_raw', 'RAW_TRANSACTIONS') }}