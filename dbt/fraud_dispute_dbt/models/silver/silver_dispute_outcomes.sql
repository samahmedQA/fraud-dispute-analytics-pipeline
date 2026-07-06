{{ config(materialized='table') }}

select
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
    datediff('day', d.opened_date, c.resolved_date) as days_to_resolution,
    current_timestamp() as modeled_at
from {{ ref('br_disputes') }} d
left join {{ ref('silver_transactions_enriched') }} t
    on d.transaction_id = t.transaction_id
left join {{ ref('br_chargeback_outcomes') }} c
    on d.dispute_id = c.dispute_id
