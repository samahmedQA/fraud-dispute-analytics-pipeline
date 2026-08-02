select
    'gold_daily_fraud_kpis' as model_name
from {{ ref('gold_daily_fraud_kpis') }}
where pipeline_run_id is null

union all

select
    'gold_daily_dispute_kpis'
from {{ ref('gold_daily_dispute_kpis') }}
where pipeline_run_id is null

union all

select
    'gold_fraud_summary_by_network'
from {{ ref('gold_fraud_summary_by_network') }}
where pipeline_run_id is null

union all

select
    'gold_dispute_chargeback_summary_by_network'
from {{ ref('gold_dispute_chargeback_summary_by_network') }}
where pipeline_run_id is null

union all

select
    'gold_pipeline_batch_metadata'
from {{ ref('gold_pipeline_batch_metadata') }}
where pipeline_run_id is null
