-- Purpose:
-- Template for testing Snowpipe auto-ingest from AWS S3 into Snowflake.
--
-- This file is safe to commit because it does not include real SQS queue ARNs,
-- AWS policy files, external IDs, or secrets.
--
-- Flow:
-- New JSON file lands in S3
--   -> S3 event notification
--   -> Snowflake Snowpipe
--   -> RAW_TRANSACTIONS_PIPE_TEST

USE ROLE ACCOUNTADMIN;
USE DATABASE FRAUD_DISPUTE_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW_TRANSACTIONS_PIPE_TEST (
  raw_record VARIANT,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE PIPE_TRANSACTIONS_SNOWPIPE_TEST
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_TRANSACTIONS_PIPE_TEST (raw_record)
FROM (
  SELECT $1
  FROM @S3_RAW_STAGE/snowpipe_test/transactions/
)
FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
PATTERN = '.*[.]json';

-- Run this after creating the pipe.
-- Snowflake returns a notification_channel value.
-- That value is used in the AWS S3 bucket notification configuration.
DESC PIPE PIPE_TRANSACTIONS_SNOWPIPE_TEST;

-- Check Snowpipe status.
SELECT SYSTEM$PIPE_STATUS('FRAUD_DISPUTE_DB.RAW.PIPE_TRANSACTIONS_SNOWPIPE_TEST');

-- Validate loaded records.
SELECT COUNT(*) AS loaded_rows
FROM RAW_TRANSACTIONS_PIPE_TEST;

SELECT
  raw_record:transaction_id::string AS transaction_id,
  loaded_at
FROM RAW_TRANSACTIONS_PIPE_TEST
ORDER BY loaded_at DESC
LIMIT 10;
