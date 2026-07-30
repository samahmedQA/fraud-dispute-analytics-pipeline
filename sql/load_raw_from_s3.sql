-- Purpose:
-- Reload Snowflake RAW tables from one validated S3 pipeline run.
--
-- Guardrails:
-- 1. The Python runner requires a valid pipeline run ID.
-- 2. Only files under raw/run_id={{RUN_ID}}/ are loaded.
-- 3. Data first lands in temporary tables with source lineage metadata.
-- 4. The runner compares temporary row and file counts with the local
--    partition manifest before allowing promotion.
-- 5. Existing RAW tables are replaced only after every validation passes.

USE ROLE FRAUD_DISPUTE_ROLE;
USE DATABASE FRAUD_DISPUTE_DB;
USE SCHEMA RAW;

ALTER TABLE RAW_CUSTOMERS
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE RAW_CUSTOMERS
    ADD COLUMN IF NOT EXISTS source_file VARCHAR;
ALTER TABLE RAW_CUSTOMERS
    ADD COLUMN IF NOT EXISTS source_row_number NUMBER;

ALTER TABLE RAW_TRANSACTIONS
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE RAW_TRANSACTIONS
    ADD COLUMN IF NOT EXISTS source_file VARCHAR;
ALTER TABLE RAW_TRANSACTIONS
    ADD COLUMN IF NOT EXISTS source_row_number NUMBER;

ALTER TABLE RAW_FRAUD_SIGNALS
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE RAW_FRAUD_SIGNALS
    ADD COLUMN IF NOT EXISTS source_file VARCHAR;
ALTER TABLE RAW_FRAUD_SIGNALS
    ADD COLUMN IF NOT EXISTS source_row_number NUMBER;

ALTER TABLE RAW_DISPUTES
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE RAW_DISPUTES
    ADD COLUMN IF NOT EXISTS source_file VARCHAR;
ALTER TABLE RAW_DISPUTES
    ADD COLUMN IF NOT EXISTS source_row_number NUMBER;

ALTER TABLE RAW_CHARGEBACK_OUTCOMES
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE RAW_CHARGEBACK_OUTCOMES
    ADD COLUMN IF NOT EXISTS source_file VARCHAR;
ALTER TABLE RAW_CHARGEBACK_OUTCOMES
    ADD COLUMN IF NOT EXISTS source_row_number NUMBER;

CREATE OR REPLACE TEMPORARY TABLE TMP_RAW_CUSTOMERS LIKE RAW_CUSTOMERS;
CREATE OR REPLACE TEMPORARY TABLE TMP_RAW_TRANSACTIONS LIKE RAW_TRANSACTIONS;
CREATE OR REPLACE TEMPORARY TABLE TMP_RAW_FRAUD_SIGNALS LIKE RAW_FRAUD_SIGNALS;
CREATE OR REPLACE TEMPORARY TABLE TMP_RAW_DISPUTES LIKE RAW_DISPUTES;
CREATE OR REPLACE TEMPORARY TABLE TMP_RAW_CHARGEBACK_OUTCOMES LIKE RAW_CHARGEBACK_OUTCOMES;

COPY INTO TMP_RAW_CUSTOMERS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
FROM (
    SELECT
        $1,
        '{{RUN_ID}}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/customers/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO TMP_RAW_TRANSACTIONS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
FROM (
    SELECT
        $1,
        '{{RUN_ID}}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/transactions/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO TMP_RAW_FRAUD_SIGNALS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
FROM (
    SELECT
        $1,
        '{{RUN_ID}}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/fraud_signals/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO TMP_RAW_DISPUTES (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
FROM (
    SELECT
        $1,
        '{{RUN_ID}}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/disputes/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO TMP_RAW_CHARGEBACK_OUTCOMES (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
FROM (
    SELECT
        $1,
        '{{RUN_ID}}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/chargeback_outcomes/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

-- The Python runner intercepts this marker and validates every temporary
-- table against partition_manifest.json. It raises before BEGIN TRANSACTION
-- when any row count, file count, run ID, or lineage field is incorrect.
SELECT '__PIPELINE_GUARDRAIL_VALIDATE_TEMP_LOAD__';

BEGIN TRANSACTION;

DELETE FROM RAW_CUSTOMERS;
DELETE FROM RAW_TRANSACTIONS;
DELETE FROM RAW_FRAUD_SIGNALS;
DELETE FROM RAW_DISPUTES;
DELETE FROM RAW_CHARGEBACK_OUTCOMES;

INSERT INTO RAW_CUSTOMERS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
SELECT
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
FROM TMP_RAW_CUSTOMERS;

INSERT INTO RAW_TRANSACTIONS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
SELECT
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
FROM TMP_RAW_TRANSACTIONS;

INSERT INTO RAW_FRAUD_SIGNALS (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
SELECT
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
FROM TMP_RAW_FRAUD_SIGNALS;

INSERT INTO RAW_DISPUTES (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
SELECT
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
FROM TMP_RAW_DISPUTES;

INSERT INTO RAW_CHARGEBACK_OUTCOMES (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
SELECT
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
FROM TMP_RAW_CHARGEBACK_OUTCOMES;

COMMIT;
