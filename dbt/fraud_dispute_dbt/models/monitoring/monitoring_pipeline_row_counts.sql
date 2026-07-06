{{ config(materialized='table', schema='MONITORING') }}

select
    'RAW' as layer,
    'RAW_CUSTOMERS' as object_name,
    count(*) as row_count,
    current_timestamp() as checked_at
from {{ source('fraud_raw', 'RAW_CUSTOMERS') }}

union all

select
    'RAW',
    'RAW_TRANSACTIONS',
    count(*),
    current_timestamp()
from {{ source('fraud_raw', 'RAW_TRANSACTIONS') }}

union all

select
    'RAW',
    'RAW_FRAUD_SIGNALS',
    count(*),
    current_timestamp()
from {{ source('fraud_raw', 'RAW_FRAUD_SIGNALS') }}

union all

select
    'RAW',
    'RAW_DISPUTES',
    count(*),
    current_timestamp()
from {{ source('fraud_raw', 'RAW_DISPUTES') }}

union all

select
    'RAW',
    'RAW_CHARGEBACK_OUTCOMES',
    count(*),
    current_timestamp()
from {{ source('fraud_raw', 'RAW_CHARGEBACK_OUTCOMES') }}

union all

select
    'SILVER',
    'SILVER_TRANSACTIONS_ENRICHED',
    count(*),
    current_timestamp()
from {{ ref('silver_transactions_enriched') }}

union all

select
    'SILVER',
    'SILVER_DISPUTE_OUTCOMES',
    count(*),
    current_timestamp()
from {{ ref('silver_dispute_outcomes') }}

union all

select
    'GOLD',
    'GOLD_FRAUD_SUMMARY_BY_NETWORK',
    count(*),
    current_timestamp()
from {{ ref('gold_fraud_summary_by_network') }}

union all

select
    'GOLD',
    'GOLD_DISPUTE_CHARGEBACK_SUMMARY_BY_NETWORK',
    count(*),
    current_timestamp()
from {{ ref('gold_dispute_chargeback_summary_by_network') }}

union all

select
    'GOLD',
    'GOLD_DAILY_FRAUD_KPIS',
    count(*),
    current_timestamp()
from {{ ref('gold_daily_fraud_kpis') }}

union all

select
    'GOLD',
    'GOLD_DAILY_DISPUTE_KPIS',
    count(*),
    current_timestamp()
from {{ ref('gold_daily_dispute_kpis') }}
