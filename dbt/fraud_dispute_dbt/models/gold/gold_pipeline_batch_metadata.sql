{{ config(materialized='table', schema='MARTS') }}

with dataset_metadata as (

    select
        pipeline_run_id,
        'customers' as dataset_name,
        count(*) as row_count,
        count(distinct source_file) as source_file_count,
        min(loaded_at) as first_loaded_at,
        max(loaded_at) as last_loaded_at
    from {{ ref('br_customers') }}
    group by pipeline_run_id

    union all

    select
        pipeline_run_id,
        'transactions',
        count(*),
        count(distinct source_file),
        min(loaded_at),
        max(loaded_at)
    from {{ ref('br_transactions') }}
    group by pipeline_run_id

    union all

    select
        pipeline_run_id,
        'fraud_signals',
        count(*),
        count(distinct source_file),
        min(loaded_at),
        max(loaded_at)
    from {{ ref('br_fraud_signals') }}
    group by pipeline_run_id

    union all

    select
        pipeline_run_id,
        'disputes',
        count(*),
        count(distinct source_file),
        min(loaded_at),
        max(loaded_at)
    from {{ ref('br_disputes') }}
    group by pipeline_run_id

    union all

    select
        pipeline_run_id,
        'chargeback_outcomes',
        count(*),
        count(distinct source_file),
        min(loaded_at),
        max(loaded_at)
    from {{ ref('br_chargeback_outcomes') }}
    group by pipeline_run_id
)

select
    pipeline_run_id,
    dataset_name,
    row_count,
    source_file_count,
    first_loaded_at,
    last_loaded_at,
    current_timestamp() as modeled_at
from dataset_metadata
