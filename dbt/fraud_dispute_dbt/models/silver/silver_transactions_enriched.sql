{{ config(materialized='table') }}

select
    t.pipeline_run_id,
    t.transaction_id,
    t.customer_id,
    t.account_id,
    c.customer_age,
    c.account_status,
    c.state,
    t.merchant_id,
    t.transaction_amount,
    t.transaction_timestamp,
    t.transaction_status,
    t.card_network,
    t.merchant_category,
    t.country,
    f.fraud_score,
    f.risk_level,
    f.rule_triggered,
    f.device_risk_score,
    f.velocity_count,
    f.model_version,
    case
        when f.risk_level = 'High' then true
        when f.fraud_score >= 0.35 then true
        else false
    end as high_risk_transaction_flag,

    t.source_file as transaction_source_file,
    t.source_row_number as transaction_source_row_number,
    t.loaded_at as transaction_loaded_at,

    c.source_file as customer_source_file,
    c.source_row_number as customer_source_row_number,
    c.loaded_at as customer_loaded_at,

    f.source_file as fraud_signal_source_file,
    f.source_row_number as fraud_signal_source_row_number,
    f.loaded_at as fraud_signal_loaded_at,

    current_timestamp() as modeled_at
from {{ ref('br_transactions') }} t
left join {{ ref('br_customers') }} c
    on t.customer_id = c.customer_id
    and t.account_id = c.account_id
    and t.pipeline_run_id = c.pipeline_run_id
left join {{ ref('br_fraud_signals') }} f
    on t.transaction_id = f.transaction_id
    and t.pipeline_run_id = f.pipeline_run_id
