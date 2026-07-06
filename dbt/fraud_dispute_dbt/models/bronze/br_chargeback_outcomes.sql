{{ config(materialized='view') }}

select
    raw_record:chargeback_id::string as chargeback_id,
    raw_record:dispute_id::string as dispute_id,
    raw_record:outcome::string as outcome,
    raw_record:win_loss_flag::string as win_loss_flag,
    raw_record:final_amount::number(12,2) as final_amount,
    raw_record:resolved_date::timestamp as resolved_date,
    raw_record:representment_required::boolean as representment_required,
    loaded_at
from {{ source('fraud_raw', 'RAW_CHARGEBACK_OUTCOMES') }}
