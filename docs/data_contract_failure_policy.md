\# Data Contract Failure Policy



\## Purpose



This document defines how the pipeline handles invalid source data before ingestion into AWS S3 and Snowflake.



The goal is not only to validate records, but to define what happens when data is wrong.



The pipeline uses versioned JSON Schema contracts to validate raw source files before they are uploaded to S3.



\## Contract Version



Current contract version:



```text

contracts/v1/

