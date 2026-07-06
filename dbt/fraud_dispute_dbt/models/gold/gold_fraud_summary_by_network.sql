{{ config(materialized='table', schema='MARTS') }}

select
    card_network,
    count(*) as total_transactions,
    count(distinct customer_id) as unique_customers,
    sum(transaction_amount) as total_transaction_amount,
    avg(transaction_amount) as avg_transaction_amount,
    avg(fraud_score) as avg_fraud_score,
    count_if(high_risk_transaction_flag = true) as high_risk_transactions,
    round(
        100.0 * count_if(high_risk_transaction_flag = true) / nullif(count(*), 0),
        2
    ) as high_risk_rate_pct,
    current_timestamp() as modeled_at
from {{ ref('silver_transactions_enriched') }}
group by card_network
