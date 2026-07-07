# Fraud & Dispute Analytics Pipeline

## Overview

This project is an end-to-end fintech analytics pipeline built with Python, AWS S3, Snowflake, and dbt.

It generates synthetic transaction, fraud signal, dispute, and chargeback data, organizes the raw files into partitioned S3-ready folders, uploads them to AWS S3, loads them into Snowflake RAW tables through an external stage, and transforms the data through dbt bronze, silver, and gold models.

The project also includes dbt data quality tests, pipeline row-count monitoring, and dbt documentation with lineage.

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
- dbt
- SQL
- JSON
- PowerShell
- Git

## Current Architecture

```text
Python Synthetic Data Generator
        ?
Local JSON Files
        ?
Partitioned Local Raw Zone
        ?
AWS S3 Raw Zone
        ?
Snowflake Storage Integration
        ?
Snowflake External Stage
        ?
Snowflake RAW Tables
        ?
dbt Bronze Models
        ?
dbt Silver Models
        ?
dbt Gold Marts
        ?
dbt Tests + Monitoring
```

## S3 Raw Zone Layout

Raw files are organized by dataset, year, and month.

Example S3 layout:

```text
raw/
+-- customers/
¦   +-- year=YYYY/month=MM/
+-- transactions/
¦   +-- year=YYYY/month=MM/
+-- fraud_signals/
¦   +-- year=YYYY/month=MM/
+-- disputes/
¦   +-- year=YYYY/month=MM/
+-- chargeback_outcomes/
    +-- year=YYYY/month=MM/
```

This layout supports cleaner backfills, dataset-level loading, and future Snowpipe automation.

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

## SQL Scripts

The `sql/` folder includes reusable Snowflake scripts:

| Script | Purpose |
|---|---|
| setup_s3_stage_template.sql | Safe template for creating a Snowflake storage integration and S3 external stage |
| load_raw_from_s3.sql | Reloads RAW tables from the S3 raw zone |
| validate_raw_counts.sql | Validates RAW table row counts after loading |
| snowflake_setup.sql | Creates core Snowflake database objects |

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

### 5. Load RAW tables from S3

Run this Snowflake script:

```text
sql/load_raw_from_s3.sql
```

### 6. Validate RAW row counts

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

### 7. Run dbt build

```powershell
cd dbt\fraud_dispute_dbt
python -c "from dbt.cli.main import cli; cli()" build
```

### 8. Generate dbt docs

```powershell
python -c "from dbt.cli.main import cli; cli()" docs generate
python -c "from dbt.cli.main import cli; cli()" docs serve
```

## Current Status

Completed:

- Synthetic fintech data generation
- Local JSON raw file generation
- S3 partitioning script
- AWS S3 raw zone upload
- Snowflake storage integration
- Snowflake external stage
- Snowflake RAW JSON loading from S3
- dbt bronze models
- dbt silver enrichment models
- dbt gold KPI marts
- dbt data quality tests
- Pipeline row-count monitoring
- dbt documentation and lineage
- Full dbt build passing successfully after S3 ingestion

## Planned Improvements

Next phases:

- Add Snowpipe auto-ingest
- Add dashboard layer
- Add GitHub Actions CI/CD
- Add screenshots to README
- Add more advanced monitoring and drift checks

## Note

This project uses fully synthetic data. It does not contain company data, customer data, production credentials, AWS policy files, or secrets.
