select
    'silver_transactions_enriched' as model_name,
    transaction_id as record_id
from {{ ref('silver_transactions_enriched') }}
where pipeline_run_id is null
   or transaction_source_file is null
   or transaction_source_row_number is null

union all

select
    'silver_dispute_outcomes',
    dispute_id
from {{ ref('silver_dispute_outcomes') }}
where pipeline_run_id is null
   or dispute_source_file is null
   or dispute_source_row_number is null
   or transaction_source_file is null
   or transaction_source_row_number is null
