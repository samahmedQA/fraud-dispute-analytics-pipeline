-- Purpose:
-- Template for creating a Snowflake storage integration and external stage
-- to read partitioned raw JSON files from AWS S3.
--
-- This file uses placeholders and is safe to commit.
-- Do not commit real AWS external IDs, trust policy files, or secrets.

USE ROLE ACCOUNTADMIN;
USE DATABASE FRAUD_DISPUTE_DB;
USE SCHEMA RAW;

-- Replace these placeholders before running:
-- <AWS_ROLE_ARN>   Example: arn:aws:iam::<aws-account-id>:role/<role-name>
-- <S3_BUCKET_NAME> Example: your-s3-bucket-name

CREATE OR REPLACE STORAGE INTEGRATION S3_FRAUD_DISPUTE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<S3_BUCKET_NAME>/raw/');

-- Run this after creating the integration.
-- Snowflake returns:
-- 1. STORAGE_AWS_IAM_USER_ARN
-- 2. STORAGE_AWS_EXTERNAL_ID
--
-- Those values are used to update the AWS IAM role trust policy.
DESC INTEGRATION S3_FRAUD_DISPUTE_INTEGRATION;

CREATE OR REPLACE FILE FORMAT JSON_LINES_FORMAT
  TYPE = JSON;

CREATE OR REPLACE STAGE S3_RAW_STAGE
  URL = 's3://<S3_BUCKET_NAME>/raw/'
  STORAGE_INTEGRATION = S3_FRAUD_DISPUTE_INTEGRATION
  FILE_FORMAT = JSON_LINES_FORMAT;

-- Test that Snowflake can see files in S3.
-- Replace YYYY and MM with an existing partition.
LIST @S3_RAW_STAGE/transactions/year=YYYY/month=MM/;
