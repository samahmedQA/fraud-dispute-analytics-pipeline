-- V2 incremental Snowflake RAW loading.
--
-- Design:
-- 1. Load one run-scoped incremental S3 batch into temporary RAW tables.
-- 2. Validate the temporary load against partition_manifest.json before
--    beginning warehouse mutation.
-- 3. MERGE each dataset into its active RAW table by source primary key.
-- 4. Insert previously unseen keys.
-- 5. Update an existing key only when the incoming updated_at is newer.
-- 6. Replaying the same or an older record is therefore a no-op.
--
-- The existing V1 full-reload loader remains in load_raw_from_s3.sql.

USE ROLE FRAUD_DISPUTE_ROLE;
USE DATABASE FRAUD_DISPUTE_DB;
USE SCHEMA RAW;

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

MERGE INTO RAW_CUSTOMERS AS target
USING TMP_RAW_CUSTOMERS AS source
ON target.raw_record:customer_id::STRING =
   source.raw_record:customer_id::STRING

WHEN MATCHED AND (
    TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    ) IS NULL
    OR TRY_TO_TIMESTAMP_NTZ(
        source.raw_record:updated_at::STRING
    ) > TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    )
)
THEN UPDATE SET
    target.raw_record = source.raw_record,
    target.pipeline_run_id = source.pipeline_run_id,
    target.source_file = source.source_file,
    target.source_row_number = source.source_row_number,
    target.loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
INSERT (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
VALUES (
    source.raw_record,
    source.pipeline_run_id,
    source.source_file,
    source.source_row_number,
    source.loaded_at
);

MERGE INTO RAW_TRANSACTIONS AS target
USING TMP_RAW_TRANSACTIONS AS source
ON target.raw_record:transaction_id::STRING =
   source.raw_record:transaction_id::STRING

WHEN MATCHED AND (
    TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    ) IS NULL
    OR TRY_TO_TIMESTAMP_NTZ(
        source.raw_record:updated_at::STRING
    ) > TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    )
)
THEN UPDATE SET
    target.raw_record = source.raw_record,
    target.pipeline_run_id = source.pipeline_run_id,
    target.source_file = source.source_file,
    target.source_row_number = source.source_row_number,
    target.loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
INSERT (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
VALUES (
    source.raw_record,
    source.pipeline_run_id,
    source.source_file,
    source.source_row_number,
    source.loaded_at
);

MERGE INTO RAW_FRAUD_SIGNALS AS target
USING TMP_RAW_FRAUD_SIGNALS AS source
ON target.raw_record:transaction_id::STRING =
   source.raw_record:transaction_id::STRING

WHEN MATCHED AND (
    TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    ) IS NULL
    OR TRY_TO_TIMESTAMP_NTZ(
        source.raw_record:updated_at::STRING
    ) > TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    )
)
THEN UPDATE SET
    target.raw_record = source.raw_record,
    target.pipeline_run_id = source.pipeline_run_id,
    target.source_file = source.source_file,
    target.source_row_number = source.source_row_number,
    target.loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
INSERT (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
VALUES (
    source.raw_record,
    source.pipeline_run_id,
    source.source_file,
    source.source_row_number,
    source.loaded_at
);

MERGE INTO RAW_DISPUTES AS target
USING TMP_RAW_DISPUTES AS source
ON target.raw_record:dispute_id::STRING =
   source.raw_record:dispute_id::STRING

WHEN MATCHED AND (
    TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    ) IS NULL
    OR TRY_TO_TIMESTAMP_NTZ(
        source.raw_record:updated_at::STRING
    ) > TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    )
)
THEN UPDATE SET
    target.raw_record = source.raw_record,
    target.pipeline_run_id = source.pipeline_run_id,
    target.source_file = source.source_file,
    target.source_row_number = source.source_row_number,
    target.loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
INSERT (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
VALUES (
    source.raw_record,
    source.pipeline_run_id,
    source.source_file,
    source.source_row_number,
    source.loaded_at
);

MERGE INTO RAW_CHARGEBACK_OUTCOMES AS target
USING TMP_RAW_CHARGEBACK_OUTCOMES AS source
ON target.raw_record:chargeback_id::STRING =
   source.raw_record:chargeback_id::STRING

WHEN MATCHED AND (
    TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    ) IS NULL
    OR TRY_TO_TIMESTAMP_NTZ(
        source.raw_record:updated_at::STRING
    ) > TRY_TO_TIMESTAMP_NTZ(
        target.raw_record:updated_at::STRING
    )
)
THEN UPDATE SET
    target.raw_record = source.raw_record,
    target.pipeline_run_id = source.pipeline_run_id,
    target.source_file = source.source_file,
    target.source_row_number = source.source_row_number,
    target.loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
INSERT (
    raw_record,
    pipeline_run_id,
    source_file,
    source_row_number,
    loaded_at
)
VALUES (
    source.raw_record,
    source.pipeline_run_id,
    source.source_file,
    source.source_row_number,
    source.loaded_at
);

COMMIT;
