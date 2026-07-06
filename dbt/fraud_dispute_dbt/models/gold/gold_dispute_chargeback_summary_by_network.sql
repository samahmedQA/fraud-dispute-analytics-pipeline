{{ config(materialized='table', schema='MARTS') }}

select
    card_network,

    count(*) as total_disputes,
    count(distinct customer_id) as unique_customers,
    sum(dispute_amount) as total_dispute_amount,
    avg(dispute_amount) as avg_dispute_amount,

    count_if(chargeback_id is not null) as total_chargebacks,
    count_if(upper(chargeback_outcome) = 'WON') as chargebacks_won,
    count_if(upper(chargeback_outcome) = 'LOST') as chargebacks_lost,
    count_if(upper(chargeback_outcome) = 'PENDING') as chargebacks_pending,

    round(
        100.0 * count_if(upper(chargeback_outcome) = 'WON')
        / nullif(count_if(chargeback_id is not null), 0),
        2
    ) as chargeback_win_rate_pct,

    sum(final_amount) as total_final_chargeback_amount,
    avg(days_to_resolution) as avg_days_to_resolution,

    current_timestamp() as modeled_at

from {{ ref('silver_dispute_outcomes') }}
group by card_network
