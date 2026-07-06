{{ config(materialized='table', schema='MARTS') }}

select
    cast(opened_date as date) as dispute_opened_date,
    card_network,
    count(*) as total_disputes,
    sum(dispute_amount) as total_dispute_amount,
    avg(dispute_amount) as avg_dispute_amount,
    count_if(chargeback_id is not null) as total_chargebacks,
    count_if(upper(chargeback_outcome) = 'WON') as chargebacks_won,
    count_if(upper(chargeback_outcome) = 'LOST') as chargebacks_lost,
    count_if(upper(chargeback_outcome) = 'PENDING') as chargebacks_pending,
    avg(days_to_resolution) as avg_days_to_resolution,
    current_timestamp() as modeled_at
from {{ ref('silver_dispute_outcomes') }}
group by 1, 2
