-- Purpose:
-- Bootstrap the Snowflake objects required for the fraud dispute analytics pipeline.
--
-- This script creates the warehouse, database, schemas, RAW landing tables
-- and JSON file format used by the controlled S3 reload process.
--
-- Schema layout:
-- RAW        = Landing tables loaded from S3 JSON files
-- STAGING    = Cleaned and transformed dbt models
-- MARTS      = Business-ready analytics models
-- MONITORING = Data quality, freshness, row count, and pipeline health models

CREATE WAREHOUSE IF NOT EXISTS FRAUD_DISPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS FRAUD_DISPUTE_DB;

USE WAREHOUSE FRAUD_DISPUTE_WH;
USE DATABASE FRAUD_DISPUTE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS MONITORING;

USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS JSON_LINES_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    raw_record VARIANT,
    pipeline_run_id VARCHAR,
    source_file VARCHAR,
    source_row_number NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    raw_record VARIANT,
    pipeline_run_id VARCHAR,
    source_file VARCHAR,
    source_row_number NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_FRAUD_SIGNALS (
    raw_record VARIANT,
    pipeline_run_id VARCHAR,
    source_file VARCHAR,
    source_row_number NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_DISPUTES (
    raw_record VARIANT,
    pipeline_run_id VARCHAR,
    source_file VARCHAR,
    source_row_number NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_CHARGEBACK_OUTCOMES (
    raw_record VARIANT,
    pipeline_run_id VARCHAR,
    source_file VARCHAR,
    source_row_number NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);