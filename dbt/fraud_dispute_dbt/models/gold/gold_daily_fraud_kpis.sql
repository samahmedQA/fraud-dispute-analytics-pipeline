{{ config(materialized='table', schema='MARTS') }}

select
    cast(transaction_timestamp as date) as transaction_date,
    card_network,
    count(*) as total_transactions,
    sum(transaction_amount) as total_transaction_amount,
    avg(transaction_amount) as avg_transaction_amount,
    avg(fraud_score) as avg_fraud_score,
    count_if(high_risk_transaction_flag = true) as high_risk_transactions,
    count(distinct customer_id) as unique_customers,
    current_timestamp() as modeled_at
from {{ ref('silver_transactions_enriched') }}
group by 1, 2
