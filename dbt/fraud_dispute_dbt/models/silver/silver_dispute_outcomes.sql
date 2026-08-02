{{ config(materialized='table') }}

select
    d.pipeline_run_id,
    d.dispute_id,
    d.claim_id,
    d.transaction_id,
    t.customer_id,
    t.account_id,
    t.transaction_amount,
    d.dispute_amount,
    d.dispute_reason,
    d.dispute_status,
    d.opened_date,
    d.card_network,
    t.fraud_score,
    t.risk_level,
    t.high_risk_transaction_flag,
    c.chargeback_id,
    c.outcome as chargeback_outcome,
    c.win_loss_flag,
    c.final_amount,
    c.resolved_date,
    c.representment_required,
    datediff(
        'day',
        d.opened_date,
        c.resolved_date
    ) as days_to_resolution,

    d.source_file as dispute_source_file,
    d.source_row_number as dispute_source_row_number,
    d.loaded_at as dispute_loaded_at,

    t.transaction_source_file,
    t.transaction_source_row_number,
    t.transaction_loaded_at,
    t.customer_source_file,
    t.customer_source_row_number,
    t.customer_loaded_at,
    t.fraud_signal_source_file,
    t.fraud_signal_source_row_number,
    t.fraud_signal_loaded_at,

    c.source_file as chargeback_source_file,
    c.source_row_number as chargeback_source_row_number,
    c.loaded_at as chargeback_loaded_at,

    current_timestamp() as modeled_at
from {{ ref('br_disputes') }} d
left join {{ ref('silver_transactions_enriched') }} t
    on d.transaction_id = t.transaction_id
    and d.pipeline_run_id = t.pipeline_run_id
left join {{ ref('br_chargeback_outcomes') }} c
    on d.dispute_id = c.dispute_id
    and d.pipeline_run_id = c.pipeline_run_id
