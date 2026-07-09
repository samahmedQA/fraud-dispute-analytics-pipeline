# Fraud & Dispute Analytics Pipeline

## Overview

This project is an end-to-end fintech analytics pipeline built with Python, AWS S3, Snowflake, Snowpipe, and dbt.

It generates synthetic transaction, fraud signal, dispute, and chargeback data, organizes the raw files into partitioned S3-ready folders, uploads them to AWS S3, loads them into Snowflake RAW tables through an external stage, and transforms the data through dbt bronze, silver, and gold models.

The project also includes manual S3 batch loading, a Snowpipe auto-ingest test, dbt data quality tests, pipeline row-count monitoring, and dbt documentation with lineage.

## Business Problem

A fintech company needs to monitor fraud risk, dispute volume, chargeback outcomes, win/loss rates, and pipeline health across card networks.

This project simulates that workflow by creating trusted reporting tables for:

- Fraud risk by card network
- Daily fraud KPIs
- Dispute and chargeback outcomes
- Chargeback win rates
- Average dispute resolution time
- Pipeline row-count monitoring

## Tech Stack

- Python
- AWS S3
- Snowflake
- Snowpipe
- dbt
- SQL
- JSON
- PowerShell
- Git

## Current Architecture

```text
Python Synthetic Data Generator
        ↓
Local JSON Files
        ↓
Partitioned Local Raw Zone
        ↓
AWS S3 Raw Zone
        ↓
Snowflake Storage Integration
        ↓
Snowflake External Stage
        ↓
Snowflake RAW Tables
        ↓
dbt Bronze Models
        ↓
dbt Silver Models
        ↓
dbt Gold Marts
        ↓
dbt Tests + Monitoring
```

## S3 Raw Zone Layout

Raw files are organized by dataset, year, and month.

Example S3 layout:

```text
raw/
├── customers/
│   └── year=YYYY/month=MM/
├── transactions/
│   └── year=YYYY/month=MM/
├── fraud_signals/
│   └── year=YYYY/month=MM/
├── disputes/
│   └── year=YYYY/month=MM/
└── chargeback_outcomes/
    └── year=YYYY/month=MM/
```

This layout supports cleaner backfills, dataset-level loading, and future automation.

## Data Sources

The project generates five synthetic datasets:

| Dataset | Description |
|---|---|
| customers | Customer and account profile data |
| transactions | Card transaction activity |
| fraud_signals | Fraud scores, rules, device risk, and velocity signals |
| disputes | Customer dispute records |
| chargeback_outcomes | Chargeback outcomes, final amounts, and resolution dates |

## Snowflake Layout

The Snowflake database is organized into four schemas:

| Schema | Purpose |
|---|---|
| RAW | Stores raw JSON records as VARIANT data |
| STAGING | Stores bronze and silver dbt models |
| MARTS | Stores business-ready gold reporting tables |
| MONITORING | Stores pipeline observability tables |


## Environment Strategy

The project supports environment separation between development and staging Snowflake targets.

Current environments:

| Environment | Snowflake Database | Purpose |
|---|---|---|
| DEV | FRAUD_DISPUTE_DB | Primary development and experimentation environment |
| STG | FRAUD_DISPUTE_STG | Clean staging validation environment used to prove the pipeline works outside development |
| PROD | Planned template | Future protected production deployment pattern |

Each environment follows the same schema layout:

```text
RAW
STAGING
MARTS
MONITORING
## dbt Model Layers

### Bronze

Bronze models flatten raw JSON records into typed relational columns.

Models:

- br_customers
- br_transactions
- br_fraud_signals
- br_disputes
- br_chargeback_outcomes

### Silver

Silver models join and enrich the bronze data.

Models:

- silver_transactions_enriched
- silver_dispute_outcomes

The silver transaction model joins transactions, customers, and fraud signals into one enriched transaction-level table. It also creates a high-risk transaction flag based on fraud score and risk level.

The silver dispute model joins disputes to enriched transactions and chargeback outcomes to create a clean dispute-level reporting table.

### Gold

Gold models are business-ready marts for reporting and dashboards.

Models:

- gold_fraud_summary_by_network
- gold_dispute_chargeback_summary_by_network
- gold_daily_fraud_kpis
- gold_daily_dispute_kpis

These marts support reporting on transaction volume, fraud risk, dispute volume, chargeback outcomes, win rates, and resolution timing.

## Data Quality

The project includes dbt tests for:

- Not-null checks
- Unique primary keys
- Accepted values
- Relationship integrity between transactions, disputes, and chargebacks

The full dbt build currently passes successfully with:

```text
PASS=38
WARN=0
ERROR=0
TOTAL=38
```

## Monitoring

The monitoring model tracks row counts across RAW, SILVER, and GOLD layers.

Model:

- monitoring_pipeline_row_counts

This table provides a lightweight observability check to confirm that key pipeline tables are populated after each dbt build.

## S3 Ingestion Milestone

The pipeline supports AWS S3-based ingestion.

Completed S3 workflow:

```text
Local JSON files
→ partitioned local raw zone
→ AWS S3 raw zone
→ Snowflake storage integration
→ Snowflake external stage
→ Snowflake RAW tables
→ dbt bronze/silver/gold models
→ dbt tests and monitoring
```

The Snowflake RAW tables were successfully loaded from the S3 raw zone and validated with the following row counts:

```text
RAW_CUSTOMERS              1500
RAW_TRANSACTIONS           10000
RAW_FRAUD_SIGNALS          10000
RAW_DISPUTES               1200
RAW_CHARGEBACK_OUTCOMES    840
```

The full dbt build passed successfully after loading from S3:

```text
PASS=38
WARN=0
ERROR=0
TOTAL=38
```

## Snowpipe Auto-Ingest Milestone

The project also includes a Snowpipe auto-ingest test flow.

Snowpipe test workflow:

```text
New JSON file uploaded to S3
→ S3 object-created event notification
→ Snowflake Snowpipe notification channel
→ Snowpipe COPY INTO execution
→ test RAW table populated automatically
```

Test objects:

| Object | Purpose |
|---|---|
| RAW_TRANSACTIONS_PIPE_TEST | Test table used to validate Snowpipe loading |
| PIPE_TRANSACTIONS_SNOWPIPE_TEST | Snowpipe object configured with AUTO_INGEST = TRUE |
| raw/snowpipe_test/transactions/ | S3 test prefix used for Snowpipe event notifications |

The Snowpipe test successfully loaded a new transaction JSON file from S3 into Snowflake automatically.

This validates an event-driven ingestion pattern in addition to the manual S3 batch load process.

## SQL Scripts

The `sql/` folder includes reusable Snowflake scripts:

| Script | Purpose |
|---|---|
| snowflake_setup.sql | Creates core Snowflake database objects |
| setup_s3_stage_template.sql | Safe template for creating a Snowflake storage integration and S3 external stage |
| load_raw_from_s3.sql | Reloads RAW tables from the S3 raw zone |
| validate_raw_counts.sql | Validates RAW table row counts after loading |
| setup_snowpipe_template.sql | Safe template for testing Snowpipe auto-ingest |

## How to Run

### 1. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 2. Generate synthetic data

```powershell
python scripts\generate_data.py
```

### 3. Partition files for S3

```powershell
python scripts\partition_data_for_s3.py
```

### 4. Upload partitioned files to S3

```powershell
aws s3 cp data/s3_partitioned/raw/ s3://<bucket-name>/raw/ --recursive
```

### 5. Set up Snowflake S3 stage

Use this safe template:

```text
sql/setup_s3_stage_template.sql
```

This creates the Snowflake storage integration, file format, and external stage.

### 6. Load RAW tables from S3

Run this Snowflake script:

```text
sql/load_raw_from_s3.sql
```

### 7. Validate RAW row counts

Run this Snowflake script:

```text
sql/validate_raw_counts.sql
```

Expected row counts:

```text
RAW_CUSTOMERS              1500
RAW_TRANSACTIONS           10000
RAW_FRAUD_SIGNALS          10000
RAW_DISPUTES               1200
RAW_CHARGEBACK_OUTCOMES    840
```

### 8. Run dbt build

```powershell
cd dbt\fraud_dispute_dbt
python -c "from dbt.cli.main import cli; cli()" build
```

### 9. Generate dbt docs

```powershell
python -c "from dbt.cli.main import cli; cli()" docs generate
python -c "from dbt.cli.main import cli; cli()" docs serve
```

### 10. Test Snowpipe auto-ingest

Use this safe template:

```text
sql/setup_snowpipe_template.sql
```

The Snowpipe test uses a separate test table and test S3 prefix so it does not affect the main RAW tables.


## Data Contracts and Failure Handling

The pipeline includes versioned data contracts to validate raw source data before ingestion into AWS S3 and Snowflake.

Current contract version:

```text
contracts/v1/
```

The pipeline now includes versioned contracts for all five raw datasets:

```text
contracts/v1/customers.schema.json
contracts/v1/transactions.schema.json
contracts/v1/fraud_signals.schema.json
contracts/v1/disputes.schema.json
contracts/v1/chargeback_outcomes.schema.json
```

These contracts enforce required fields, expected data types, valid enum values, ID patterns, date/timestamp formats, and numeric boundaries before raw data is allowed to continue toward S3 ingestion.

Validation is performed before S3 upload using:

```powershell
python scripts/validate_data_contracts.py
```

Current full validation result:

```text
Dataset: chargeback_outcomes
Status: PASSED
Total Records: 840
Invalid Records: 0

Dataset: customers
Status: PASSED
Total Records: 1500
Invalid Records: 0

Dataset: disputes
Status: PASSED
Total Records: 1200
Invalid Records: 0

Dataset: fraud_signals
Status: PASSED
Total Records: 10000
Invalid Records: 0

Dataset: transactions
Status: PASSED
Total Records: 10000
Invalid Records: 0

Validator exit code was 0
```

### Severity-Based Failure Design

The project does not treat every data issue the same way. It uses severity tiers to decide how the pipeline should respond when data is wrong.

| Severity | Example | Pipeline Behavior |
|---|---|---|
| hard_fail | Missing required field, wrong data type, invalid enum value, duplicate primary key | Quarantine invalid records, write a validation report, mark batch as FAILED, and block S3 upload |
| quarantine_continue | Invalid child record, such as a chargeback referencing a missing dispute | Quarantine the invalid child records, write a validation report, and allow valid records to continue |
| warn_continue | Late-arriving event or unusually old transaction date | Write a warning to the validation report and continue the pipeline |

For structural contract violations, the project uses a strict batch-level hard-fail policy. If any transaction record has a `hard_fail`, the entire dataset batch is blocked from S3 upload. This is intentional because structural issues may indicate the upstream source or generator is broken, not just one isolated record.

### Validation Report

Each validation run writes a detailed report to:

```text
data/validation_reports/
```

The report includes:

- Dataset name
- Contract version
- Batch status
- Pipeline action
- Total record count
- Valid record count
- Invalid record count
- Warning count
- Failed rule details
- Severity for each failed rule

Example failed rule output:

```text
transactions  TXN_99999999  card_network        enum  hard_fail  'Discover' is not one of ['Mastercard', 'Pulse', 'Visa']
transactions  TXN_99999999  transaction_amount  type  hard_fail  'one hundred' is not of type 'number'
```

### Quarantine Handling

Invalid records are written to:

```text
data/quarantine/invalid_records/
```

These files are ignored by Git because they are generated validation artifacts.

### Repeatable Bad-Data Test

A repeatable fixture proves the failure path:

```text
tests/fixtures/bad_transactions.json
```

Run the failure test with:

```powershell
python scripts/validate_data_contracts.py --dataset transactions --input-file tests\fixtures\bad_transactions.json
```

Expected result:

```text
Dataset: transactions
Status: FAILED
Pipeline Action: BLOCK_S3_UPLOAD
Total Records: 2
Invalid Records: 1
Warnings: 0
```

This proves the pipeline can detect invalid data, quarantine the bad record, write a debuggable validation report, return a failing exit code, and block ingestion before bad data reaches S3 or Snowflake.


## Current Status

Completed:

- Synthetic fintech data generation
- Local JSON raw file generation
- S3 partitioning script
- AWS S3 raw zone upload
- Snowflake storage integration
- Snowflake external stage
- Snowflake RAW JSON loading from S3
- Snowpipe auto-ingest test
- dbt bronze models
- dbt silver enrichment models
- dbt gold KPI marts
- dbt data quality tests
- Pipeline row-count monitoring
- dbt documentation and lineage
- Full dbt build passing successfully after S3 ingestion

## Planned Improvements

Next phases:

- Expand Snowpipe from a test prefix to full dataset ingestion
- Add dashboard layer
- Add GitHub Actions CI/CD
- Add screenshots to README
- Add more advanced monitoring and drift checks

## Note

This project uses fully synthetic data. It does not contain company data, customer data, production credentials, AWS policy files, Snowflake external IDs, or secrets.