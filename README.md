\# Fraud \& Dispute Analytics Pipeline



\## Overview



This project is an end-to-end fintech analytics pipeline built with Python, Snowflake, and dbt.



It generates synthetic transaction, fraud signal, dispute, and chargeback data, loads raw JSON records into Snowflake, transforms the data through dbt bronze, silver, and gold models, and creates reporting marts for fraud and dispute analytics.



The project also includes dbt data quality tests, pipeline row-count monitoring, and dbt documentation with lineage.



\## Current Architecture



```text

Python Synthetic Data Generator

&#x20;       ↓

Local JSON Files

&#x20;       ↓

Snowflake RAW Tables

&#x20;       ↓

dbt Bronze Models

&#x20;       ↓

dbt Silver Models

&#x20;       ↓

dbt Gold Marts

&#x20;       ↓

dbt Tests + Monitoring

&#x20;       ↓

dbt Docs Lineage

```



\## Current Status



Completed:



\* Synthetic fintech data generation

\* Snowflake RAW JSON loading

\* dbt bronze models

\* dbt silver enrichment models

\* dbt gold KPI marts

\* dbt data quality tests

\* Pipeline row-count monitoring

\* dbt documentation and lineage

\* Full dbt build passing successfully



Latest dbt build result:



```text

PASS=38

WARN=0

ERROR=0

```



\## Planned Improvements



Next phases:



\* Add AWS S3 ingestion

\* Add Snowflake external stage

\* Add Snowpipe auto-ingest

\* Add dashboard layer

\* Add GitHub Actions CI/CD

\* Add more advanced monitoring and drift checks



\## Note



This project uses fully synthetic data. It does not contain company data, customer data, or production credentials.



