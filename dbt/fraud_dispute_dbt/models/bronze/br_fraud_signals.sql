{{ config(materialized='view') }}

select
    raw_record:transaction_id::string as transaction_id,
    raw_record:fraud_score::float as fraud_score,
    raw_record:risk_level::string as risk_level,
    raw_record:rule_triggered::string as rule_triggered,
    raw_record:device_risk_score::float as device_risk_score,
    raw_record:velocity_count::number as velocity_count,
    raw_record:model_version::string as model_version,
    raw_record:score_timestamp::timestamp as score_timestamp,
    loaded_at
from {{ source('fraud_raw', 'RAW_FRAUD_SIGNALS') }}
