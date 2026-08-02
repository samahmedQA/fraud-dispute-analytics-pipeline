select
    'br_customers' as model_name,
    customer_id as record_id
from {{ ref('br_customers') }}
where pipeline_run_id is null
   or source_file is null
   or source_row_number is null
   or loaded_at is null

union all

select
    'br_transactions',
    transaction_id
from {{ ref('br_transactions') }}
where pipeline_run_id is null
   or source_file is null
   or source_row_number is null
   or loaded_at is null

union all

select
    'br_fraud_signals',
    transaction_id
from {{ ref('br_fraud_signals') }}
where pipeline_run_id is null
   or source_file is null
   or source_row_number is null
   or loaded_at is null

union all

select
    'br_disputes',
    dispute_id
from {{ ref('br_disputes') }}
where pipeline_run_id is null
   or source_file is null
   or source_row_number is null
   or loaded_at is null

union all

select
    'br_chargeback_outcomes',
    chargeback_id
from {{ ref('br_chargeback_outcomes') }}
where pipeline_run_id is null
   or source_file is null
   or source_row_number is null
   or loaded_at is null
