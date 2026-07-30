-- Purpose:
-- Validate RAW row counts, file counts, and lineage by pipeline run ID.

USE ROLE FRAUD_DISPUTE_ROLE;
USE DATABASE FRAUD_DISPUTE_DB;
USE SCHEMA RAW;

SELECT
    'RAW_CHARGEBACK_OUTCOMES' AS table_name,
    pipeline_run_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT source_file) AS source_file_count,
    MIN(loaded_at) AS first_loaded_at,
    MAX(loaded_at) AS last_loaded_at
FROM RAW_CHARGEBACK_OUTCOMES
GROUP BY pipeline_run_id

UNION ALL

SELECT
    'RAW_CUSTOMERS',
    pipeline_run_id,
    COUNT(*),
    COUNT(DISTINCT source_file),
    MIN(loaded_at),
    MAX(loaded_at)
FROM RAW_CUSTOMERS
GROUP BY pipeline_run_id

UNION ALL

SELECT
    'RAW_DISPUTES',
    pipeline_run_id,
    COUNT(*),
    COUNT(DISTINCT source_file),
    MIN(loaded_at),
    MAX(loaded_at)
FROM RAW_DISPUTES
GROUP BY pipeline_run_id

UNION ALL

SELECT
    'RAW_FRAUD_SIGNALS',
    pipeline_run_id,
    COUNT(*),
    COUNT(DISTINCT source_file),
    MIN(loaded_at),
    MAX(loaded_at)
FROM RAW_FRAUD_SIGNALS
GROUP BY pipeline_run_id

UNION ALL

SELECT
    'RAW_TRANSACTIONS',
    pipeline_run_id,
    COUNT(*),
    COUNT(DISTINCT source_file),
    MIN(loaded_at),
    MAX(loaded_at)
FROM RAW_TRANSACTIONS
GROUP BY pipeline_run_id

ORDER BY table_name, pipeline_run_id;
