# Data Contract Failure Policy

## Purpose

This document defines how the pipeline handles invalid source data before ingestion into AWS S3 and Snowflake.

The goal is not only to validate records, but to define what happens when data is wrong.

The pipeline uses versioned JSON Schema contracts to validate raw source files before they are uploaded to S3.

## Contract Version

The pipeline supports versioned source contracts:

```text
contracts/v1/
contracts/v2/
```

V1 contracts are preserved unchanged for historical replay compatibility.

Newly generated snapshots declare:

```text
contract_version=v2
```

in `raw_manifest.json`.

If a historical manifest does not contain `contract_version`, validation intentionally falls back to V1.

V2 adds required `updated_at` source change-tracking semantics. `updated_at` represents the source record's last-change timestamp and must not be earlier than the dataset's lifecycle timestamp.

Contract versioning is independent of `pipeline_run_id`. A pipeline run identifies which execution processed a record; `updated_at` identifies when the source record itself last changed.

## Validation Severity

The validator distinguishes between hard failures, quarantinable relationship failures, and warnings so downstream behavior is proportional to the defect.

- `hard_fail`: structural or contract failures that block downstream progression.
- `quarantine_continue`: invalid child records are quarantined while valid records may continue.
- `warn_continue`: operational warnings are recorded without blocking valid records.

Validation reports and quarantine outputs remain run scoped so failed or replayed batches can be diagnosed without losing source lineage.
