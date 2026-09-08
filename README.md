# Fraud & Dispute Analytics Data Platform

[![CI](https://github.com/samahmedQA/fraud-dispute-analytics-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/samahmedQA/fraud-dispute-analytics-pipeline/actions/workflows/ci.yml)

Production-style stateful incremental data platform for fraud, disputes, and chargebacks, built around reliability, replay, incremental change processing, and failure-safe state management.

The platform generates and processes **23,540 synthetic fintech records** across **5 source datasets** governed by **versioned V1/V2 JSON Schema contracts**, preserves immutable run-scoped inputs, validates data before external side effects, and carries pipeline lineage into Snowflake and downstream dbt models.

**Latest release:** `v2.0.0-incremental-platform` | **V1:** `v1.0.0-data-platform`

**V2 highlights:** Stateful incremental selection, per-dataset watermarks, sparse publication, idempotent Snowflake `MERGE`, and failure-safe checkpoint commits.

> This is a portfolio project built entirely with synthetic data. It contains no company data, customer data, credentials, or secrets.

---

## At a Glance

| Metric | Current repository |
|---|---:|
| Synthetic records | **23,540** |
| Source datasets | **5** |
| JSON Schema contract files | **10 (5 V1 + 5 V2)** |
| pytest cases | **140** |
| dbt models | **13** |
| Gold models | **5** |
| Snowflake schemas | **4** |
| External-system execution | **Dry-run by default** |

**Technology stack:** Python · AWS S3 · Snowflake · dbt · Apache Airflow · Docker · GitHub Actions · pytest · Streamlit

V2 evolves the reliable V1 batch foundation into stateful incremental processing. The engineering focus is reliable execution: stable inputs, explicit run identity, validation boundaries, safe replay, failure isolation, guarded publication, and auditable outcomes.

---

## V2 Architecture

```mermaid
flowchart LR
 A[Raw Snapshot] --> B[Validation]
 B --> C[Incremental Selection]
 C --> D[Sparse Partitioning]
 D --> E[S3 Publication]
 E --> F[Snowflake MERGE]
 F --> G[Required Stages Succeed]
 G --> H[Commit Checkpoint Last]
 C --> I[Candidate Checkpoint]
 H -. next watermark .-> C
```

V2 processes only new or changed records and supports zero-change batches. Committed ingestion state advances only after required downstream stages succeed, allowing failed runs to retry safely.

## Key Engineering Decisions

| Decision | Engineering rationale |
|---|---|
| **Immutable run-scoped raw snapshots** | A replay reads the same owned input batch instead of whatever files happen to exist later. |
| **Deterministic generation + manifest verification** | A seeded run can be reproduced, while row counts, file sizes, and SHA-256 hashes detect changed or incomplete input snapshots. |
| **Validation before external side effects** | Contract and integrity failures are resolved before the pipeline can mutate S3 or Snowflake. |
| **Severity-aware data quality** | Structural corruption, quarantinable relationship failures, and operational warnings produce different pipeline actions instead of one generic failure mode. |
| **Valid-parent-only referential integrity** | A child record cannot pass integrity checks merely because its referenced parent exists physically; the parent must itself be valid. |
| **Composite customer/account integrity** | The pipeline validates the `customer_id` + `account_id` relationship, preventing individually valid identifiers from forming an invalid pair. |
| **Run-scoped lineage** | `pipeline_run_id`, source file, source row number, and load metadata make warehouse records traceable back to a specific batch and source record. |
| **Idempotent S3 publication** | Completed identical run prefixes can be recognized safely; partial or conflicting prefixes are blocked unless replacement is explicit. |
| **Guarded Snowflake loading** | Data first lands in temporary RAW tables and must satisfy manifest and lineage checks before warehouse mutation. V1 performs guarded full replacement; V2 adds incremental primary-key, duplicate-key, and `updated_at` validation before `MERGE`. |
| **Dry-run external execution** | S3 and Snowflake mutations require explicit execute flags, making local development and CI safe by default. |
| **Recoverable, auditable runs** | Run IDs, validation reports, quarantine outputs, step-level audit records, and failure metadata preserve enough context to diagnose and replay a batch. |

These choices are the core of the project: the platform is designed around what happens when data is wrong, a run is replayed, a publication is incomplete, or the same batch is executed again.

---

## V2 Incremental Proof

**Baseline:** 23,540 records received -> 23,540 selected

**Identical replay:** 23,540 received -> 0 selected

**One customer changed:** 23,540 received -> 1 selected

**Committed version replayed:** 23,540 received -> 0 selected

### Failure Recovery

Checkpoint state advances only after required downstream stages succeed. If S3, Snowflake, or requested dbt work fails, the candidate checkpoint remains uncommitted and the prior watermark is preserved for safe retry.

---

## Business Problem

A fintech data platform needs trustworthy analytics for fraud risk, dispute volume, chargeback outcomes, win/loss rates, resolution timing, and pipeline health across card networks.

This project simulates that domain and builds reliable reporting layers for:

- Fraud risk and transaction activity by card network
- Daily fraud KPIs
- Dispute and chargeback outcomes
- Chargeback win/loss rates
- Average dispute resolution time
- Pipeline and batch-level monitoring

The analytical outputs matter, but this is primarily an engineering project: it demonstrates how data is generated, validated, published, loaded, traced, and recovered before it becomes a dashboard metric.

---

## Data Model & Scale

The platform generates five related synthetic datasets.

| Dataset | Description | Records |
|---|---|---:|
| `customers` | Customer and account profile data | 1,500 |
| `transactions` | Card transaction activity | 10,000 |
| `fraud_signals` | Fraud scores, rules, device risk, and velocity signals | 10,000 |
| `disputes` | Customer dispute records | 1,200 |
| `chargeback_outcomes` | Chargeback outcomes, final amounts, and resolution dates | 840 |
| **Total** |  | **23,540** |

Core relationships are intentionally cross-dataset:

```text
customers
   │ customer_id + account_id
   ▼
transactions ─────────────► fraud_signals
   │ transaction_id
   ▼
disputes
   │ dispute_id
   ▼
chargeback_outcomes
```

This gives the validation layer meaningful integrity work rather than five independent files.

---

## Quick Start

### 1. Install the reproducible development environment

From the repository root:

```powershell
python -m pip install --require-hashes -r requirements-dev.lock.txt
```

The hash-locked development file is the preferred installation path for reproducing the environment used by CI. External AWS, Snowflake, and dbt execution still require local configuration and credentials.

### 2. Run the safe local pipeline

```powershell
python scripts/pipeline.py run
```

The default run performs local generation, validation, and partitioning. External S3 and Snowflake stages are not executed unless they are explicitly requested and their execute flags are supplied.

### 3. Preview the V2 incremental path

```powershell
python scripts/pipeline.py run --mode incremental
```

This runs V2 locally in safe mode. Candidate state stays uncommitted unless required downstream execution succeeds.

### 4. Replay an existing immutable raw snapshot

Replace the example with a real run ID already present under `data/raw/<run_id>/`:

```powershell
$runId = "20260731T190000Z_a1b2c3d4"

python scripts/pipeline.py run `
  --run-id $runId `
  --skip-generate
```

### 5. Run the automated tests

```powershell
python -m pytest tests -q
```

The repository contains **140 pytest cases** covering pipeline reliability, CLI behavior, semantic validation, source change tracking, incremental selection and checkpoint recovery, sparse incremental publication and zero-change batch handling, V1/V2 contract compatibility, referential integrity, S3 idempotency, guarded Snowflake full loading, incremental `MERGE` behavior, sparse and zero-change warehouse manifests, dbt lineage assertions, supported loader behavior, and documentation alignment.

For stage-by-stage commands and external-system configuration, continue into the technical deep dive below.

---

# Technical Deep Dive

## V1 Full-Batch Technical Flow

```mermaid
flowchart TD
    GEN["Synthetic Data Generation"]
    RAW["Immutable Raw Snapshot<br/>run ID + manifest + SHA-256"]
    DQ["Data Quality Gate"]
    VALID["Validated Data"]
    QUAR["Quarantine + Reports"]
    PART["Run-Scoped Partitioning"]
    S3["Idempotent S3 Publication"]
    TMP["Temporary Snowflake RAW Load"]
    GUARD["Load Guardrails"]
    RAWDB["Transactional RAW Promotion"]
    DBT["dbt<br/>Bronze → Silver → Gold"]
    OUT["Analytics + Monitoring<br/>Streamlit"]

    GEN --> RAW
    RAW --> DQ
    DQ -->|valid| VALID
    DQ -->|invalid| QUAR
    VALID --> PART
    PART --> S3
    S3 --> TMP
    TMP --> GUARD
    GUARD --> RAWDB
    RAWDB --> DBT
    DBT --> OUT
```

This diagram documents the preserved V1 full-batch data path. Invalid records are routed to run-scoped quarantine and validation reports.

The current Airflow DAG ends after `partition_validated_data`; S3 publication, Snowflake loading, and dbt are not current Airflow tasks.

## Run-Scoped Data Lifecycle

Every batch is owned by a pipeline run ID with the format:

```text
YYYYMMDDTHHMMSSZ_aaaaaaaa
```

The local lifecycle is run scoped:

```text
data/raw/<run_id>/
        │
        │ raw_manifest.json + source JSONL
        ▼
contract and integrity validation
        │
        ├──────────────► data/validation_reports/<run_id>/
        ├──────────────► data/quarantine/<run_id>/invalid_records/
        │
        ▼
data/validated/<run_id>/
        │
        ▼
data/s3_partitioned/<run_id>/raw/<dataset>/year=YYYY/month=MM/
        │
        ▼
s3://<bucket>/raw/run_id=<run_id>/<dataset>/year=YYYY/month=MM/...
```

Only validated records are eligible for partitioning. A quarantined record does not silently continue downstream.

In V2, incremental selection writes run-scoped output under `data/incremental/<run_id>/`, while committed ingestion state is stored separately in `data/incremental_state/checkpoint.json` and advances only after required downstream success.

The stage-oriented CLI exposes the lifecycle directly:

```powershell
$runId = "20260731T190000Z_a1b2c3d4"

python scripts/pipeline.py validate `
  --run-id $runId

python scripts/pipeline.py partition `
  --run-id $runId
```

The end-to-end local runner also writes a run-level audit record so the final status and failed step are recoverable after execution.

---

## Data Contracts & Quality Gates

Versioned JSON Schema contracts live under:

```text
contracts/v1/
contracts/v2/
```

V1 contracts are preserved unchanged for historical replay compatibility. V2 contains the same five dataset contracts with required `updated_at` source change tracking.

```text
customers.schema.json
transactions.schema.json
fraud_signals.schema.json
disputes.schema.json
chargeback_outcomes.schema.json
```

Validation covers more than JSON shape. The pipeline enforces:

- Required fields and expected data types
- Enum values, identifier patterns, and numeric boundaries
- Real calendar dates and timestamps rather than regex shape alone
- Duplicate primary-key detection
- Cross-dataset referential integrity
- Composite `customer_id` / `account_id` integrity
- Validation against **schema-valid parent records**
- Severity-based hard-fail, quarantine, and warning policies

Before dataset validation begins, the validator verifies the raw snapshot manifest for the requested run. That prevents validation from unknowingly reading a missing, modified, or mismatched raw batch.

New V2 manifests declare `contract_version=v2`. Historical manifests without `contract_version` intentionally resolve to V1 for backward-compatible replay. In V2, `updated_at` represents the source record's last-change timestamp, is initialized deterministically from the existing lifecycle timestamp, and cannot be earlier than that lifecycle timestamp. It is independent of `pipeline_run_id` and warehouse load time.

### Severity policy

| Severity | Example | Pipeline behavior |
|---|---|---|
| `hard_fail` | Missing required field, invalid type/enum, duplicate primary key | Quarantine invalid records, write reports, fail the batch, and block downstream progression |
| `quarantine_continue` | Structurally valid child references a missing or invalid parent | Quarantine invalid child records and allow valid records to continue |
| `warn_continue` | Late-arriving or unusually old transaction event | Record the warning and continue |

This keeps operational behavior proportional to the defect instead of treating every anomaly as either fatal or harmless.

### Reports and quarantine

Validation reports are written to:

```text
data/validation_reports/<run_id>/
```

Invalid records are written to:

```text
data/quarantine/<run_id>/invalid_records/
```

Validated output is written to:

```text
data/validated/<run_id>/
```

Each dataset report records items such as:

- Dataset and contract version
- Batch status and pipeline action
- Total, valid, invalid, and warning counts
- Failed rule details and severity
- Quarantine file path, when applicable
- Validated file path, when applicable

The failure policy is also documented in:

```text
docs/data_contract_failure_policy.md
```

---

## Deterministic Generation & Replay

Synthetic data generation is seeded so the same generation parameters produce a reproducible dataset. Each new raw snapshot is written beneath its owning run ID:

```text
data/raw/<run_id>/
```

The snapshot includes `raw_manifest.json`. The manifest records:

- Manifest version
- Contract version
- Pipeline run ID
- Deterministic seed
- Base date
- Generation timestamp
- Dataset count
- Total record count
- Per-file record count
- Per-file size
- Per-file SHA-256 hash

If a run-scoped raw directory already exists, generation does not silently replace it. The existing manifest and source files are verified against the requested run and seed before the snapshot is accepted for reuse.

This makes replay explicit:

```powershell
$runId = "20260731T190000Z_a1b2c3d4"

python scripts/pipeline.py run `
  --run-id $runId `
  --skip-generate
```

Replay therefore means “process this exact owned snapshot again,” not “regenerate approximately the same type of data.”

---

## S3 Publication & Idempotency

Validated records are partitioned locally by dataset, year, and month beneath the run-scoped partition directory:

```text
data/s3_partitioned/<run_id>/raw/
├── customers/year=YYYY/month=MM/
├── transactions/year=YYYY/month=MM/
├── fraud_signals/year=YYYY/month=MM/
├── disputes/year=YYYY/month=MM/
└── chargeback_outcomes/year=YYYY/month=MM/
```

The partition step also writes a `partition_manifest.json` describing the run and partitioned output.

Remote publication adds the run ID before the dataset path:

```text
s3://<bucket>/raw/run_id=<run_id>/...
```

The S3 publisher validates local partition output before publication and uses metadata to make reruns explicit. The publication design includes:

- Run-scoped remote prefixes
- Partition-manifest verification
- SHA-256 metadata for published files
- An inventory hash for the expected run contents
- `_SUCCESS.json` as the completion marker
- Detection of an already-complete identical run
- Detection of partial or conflicting remote prefixes
- Explicit `--allow-overwrite` semantics for intentional replacement

Preview one publication without mutating AWS:

```powershell
$runId = "20260731T190000Z_a1b2c3d4"

python scripts/pipeline.py upload-s3 `
  --run-id $runId `
  --bucket <your-bucket-name>
```

The command is a local dry run unless `--execute` is supplied. An intentional replacement additionally requires the overwrite option supported by the CLI.

The dry run validates publication preparation; it does **not** contact AWS or by itself prove live S3 integration.

---

## Snowflake Loading & Recovery

The Snowflake database is organized into four schemas:

| Schema | Purpose |
|---|---|
| `RAW` | Raw JSON landing records plus source lineage |
| `STAGING` | Bronze and silver transformation layers |
| `MARTS` | Gold analytical marts |
| `MONITORING` | Pipeline observability outputs |

The bootstrap SQL is located at:

```text
sql/snowflake_setup.sql
```

It defines the core warehouse, database, schemas, JSON file format, and five RAW landing tables.

### Clone and adapt this platform

This repository is designed as a reusable reference implementation rather than a deployment tied to one cloud account.

A team can replace environment-specific configuration, source contracts, and business models while keeping the reliability framework.

Recommended bootstrap order:

1. Run `sql/snowflake_setup.sql` to create the core Snowflake objects.
2. Configure `sql/setup_s3_stage_template.sql` with the AWS role ARN and S3 bucket.
3. Configure `sql/setup_snowflake_role_template.sql` with the target Snowflake user.
4. For pre-V2 deployments only, run `sql/migrate_v2_lineage_columns.sql` with an admin role.
5. Replace the synthetic sources, contracts, and dbt models with organization-specific implementations.

The reusable framework keeps data contracts, incremental checkpoints, idempotent S3 publication, guarded Snowflake loading, dbt testing, lineage, audit logging, and safe failure recovery.

### Run-specific guarded RAW load

The supported loader consumes a single published S3 run:

```text
raw/run_id=<run_id>/...
```

Dry run:

```powershell
$runId = "20260731T190000Z_a1b2c3d4"

python scripts/pipeline.py load-snowflake `
  --run-id $runId
```

Execute only after the target environment is configured:

```powershell
python scripts/pipeline.py load-snowflake `
  --run-id $runId `
  --execute
```

The default SQL file is:

```text
sql/load_raw_from_s3.sql
sql/merge_raw_from_s3.sql
```

The loader follows a guarded promotion sequence:

```text
COPY one run into temporary RAW tables
        ↓
validate expected row counts
validate source file counts
validate pipeline_run_id
validate source_file / source_row_number lineage
        ↓
BEGIN TRANSACTION
        ↓
replace active RAW contents from validated temporary tables
        ↓
COMMIT
```

### V2 incremental RAW loading

V2 keeps the guarded V1 full-reload path intact and adds a separate incremental warehouse path. The CLI selects `sql/merge_raw_from_s3.sql` automatically when incremental mode is requested.

Dry-run an incremental load:

```powershell
python scripts/pipeline.py load-snowflake `
  --run-id $runId `
  --mode incremental
```

Execute against a configured Snowflake target:

```powershell
python scripts/pipeline.py load-snowflake `
  --run-id $runId `
  --mode incremental `
  --execute
```

For the recovery-aware end-to-end V2 path:

```powershell
python scripts/pipeline.py run `
  --run-id $runId `
  --skip-generate `
  --mode incremental `
  --upload-s3 `
  --bucket $env:FRAUD_DISPUTE_S3_BUCKET `
  --execute-s3 `
  --load-snowflake `
  --execute-snowflake
```

The committed checkpoint advances only after executed S3 publication and the Snowflake `MERGE` succeed, plus dbt when that downstream stage is requested. If a required downstream stage fails, the candidate checkpoint remains uncommitted and the previous committed watermark is preserved for safe retry.

Incremental manifests may contain sparse datasets or a fully zero-change batch. Before warehouse mutation, temporary RAW tables are checked against expected row and file counts, pipeline run lineage, required source metadata, dataset primary keys, duplicate primary keys, and parseable `updated_at` values.

The five RAW merges use the source business keys: `customer_id`, `transaction_id`, `transaction_id`, `dispute_id`, and `chargeback_id`. Unseen keys are inserted. Existing keys are updated only when the incoming `updated_at` is newer than the warehouse version, so replaying the same batch or receiving an older version does not overwrite newer state.

The incremental loader is covered by automated tests and has also been verified against live AWS S3 and Snowflake infrastructure. Live runs have exercised baseline loading, one-record incremental `MERGE`, zero-change batches, idempotent replay, and downstream failure/retry recovery without advancing the committed checkpoint prematurely.

V2 warehouse guardrails run **before** any target-table `MERGE`. If staged data does not match the local partition manifest or required lineage expectations, promotion is blocked.

RAW lineage fields include:

```text
pipeline_run_id
source_file
source_row_number
loaded_at
```

### Controlled full-reload tradeoff

V1 uses a controlled full-RAW replacement pattern because the project is small, synthetic, batch oriented, and optimized for deterministic replay. The important property is not “full reload”; it is that replacement occurs only after a run-specific staged load passes guardrails.

V2 preserves that V1 path for backward compatibility while adding a separate incremental `MERGE` path. Incremental loads may be sparse, including legitimate zero-row datasets, and warehouse rows are matched by dataset primary key with `updated_at` used to prevent same-version replays or stale records from replacing newer state.

A materially larger append-oriented production workload would likely require different state-management and incremental-ingestion semantics. Those are production considerations, not requirements to make this V1 portfolio dataset artificially complex.

---

## dbt Transformation Layer

The dbt project contains **13 SQL models**:

```text
5 Bronze
2 Silver
5 Gold
1 Monitoring
```

### Bronze — 5 models

Bronze models flatten RAW JSON into typed relational columns while preserving source lineage.

```text
br_customers
br_transactions
br_fraud_signals
br_disputes
br_chargeback_outcomes
```

Each Bronze model carries lineage fields including `pipeline_run_id`, `source_file`, `source_row_number`, and `loaded_at`.

### Silver — 2 models

```text
silver_transactions_enriched
silver_dispute_outcomes
```

`silver_transactions_enriched` joins transactions, customers, and fraud signals at the same pipeline-run grain and adds analytical fraud attributes.

`silver_dispute_outcomes` combines disputes, enriched transaction context, and chargeback outcomes while preserving batch-aware joins and source provenance.

### Gold — 5 models

```text
gold_fraud_summary_by_network
gold_dispute_chargeback_summary_by_network
gold_daily_fraud_kpis
gold_daily_dispute_kpis
gold_pipeline_batch_metadata
```

The first four provide business-facing fraud, dispute, chargeback, and timing metrics while retaining `pipeline_run_id` in their aggregation grain. `gold_pipeline_batch_metadata` summarizes batch-level dataset metadata across all five Bronze datasets.

### Monitoring — 1 model

```text
monitoring_pipeline_row_counts
```

The monitoring model provides a lightweight row-count view across core warehouse layers.

### dbt tests and execution claim

The project includes dbt schema tests and custom lineage tests, including assertions for Bronze, Silver, and Gold batch lineage. The repository also includes pytest assertions that inspect the dbt model SQL for expected lineage behavior.

A **current successful live `dbt build` is not claimed in this README** because execution depends on a configured Snowflake target and no current build artifact is being used as evidence for this PR.

A safe profile template is provided at:

```text
dbt/fraud_dispute_dbt/profiles.yml.example
```

The real dbt profile belongs outside the repository, normally at:

```text
~/.dbt/profiles.yml
```

---

## Airflow Orchestration

The local Airflow DAG is defined at:

```text
airflow/dags/fraud_dispute_pipeline_dag.py
```

Its current task graph is exactly:

```text
create_pipeline_run_id
→ generate_synthetic_data
→ validate_data_contracts
→ partition_validated_data
```

Airflow creates one run ID and passes that identifier through the local stages. Dataset files remain in the shared Docker volume rather than being pushed through XCom; XCom carries only the run ID.

**S3 publication, Snowflake loading, and dbt execution are not currently Airflow tasks.** Those capabilities are exposed by the stage-oriented CLI and are intentionally outside the current DAG.

The local Compose environment includes PostgreSQL metadata storage, a scheduler, API server, DAG processor, retries, task execution timeouts, and a one-active-run DAG constraint. It is a local orchestration demonstration, not a production Airflow deployment.

Airflow-specific setup and security limitations are documented in:

```text
airflow/README.md
```

---

## CI/CD and Docker

GitHub Actions is configured in:

```text
.github/workflows/ci.yml
```

The workflow has two jobs.

### Python checks

The Python job is configured to:

```text
checkout
→ set up the repository Python version
→ install requirements-dev.lock.txt with --require-hashes
→ compile Python files
→ compile the Airflow DAG
→ run the full pytest suite
→ create a run ID
→ generate synthetic data
→ validate data contracts
→ partition validated data
```

CI deliberately stops before external mutations:

```text
No real S3 upload
No Snowflake reload execution
No cloud credentials required
```

### Docker checks

The Docker job is configured to:

```text
build the test target
→ run containerized tests
→ build the runtime target
→ verify the runtime CLI
→ verify the runtime runs as non-root UID 10001
```

The root `Dockerfile` is multi-stage and uses the hash-locked dependency files. The runtime image is separated from the test image so test-only dependencies do not define the production-style runtime surface.

This README describes what CI is configured to verify; the live GitHub Actions badge above is the appropriate source for current workflow status.

---

## Reliability & Observability

Reliability signals are produced at multiple stages rather than only at the dashboard layer.

### Validation audit log

Each validation run writes:

```text
data/validation_reports/<run_id>/validation_audit_log.jsonl
```

Records include:

- Validation timestamp
- Dataset and contract version
- Batch status and pipeline action
- Total, valid, invalid, and warning counts
- Error counts by severity
- Report, quarantine, and validated-output paths

### Pipeline audit log

The local end-to-end runner writes run-level audit artifacts under:

```text
data/pipeline_audit_logs/
```

A pipeline audit record includes:

- Pipeline run ID
- Start and end timestamps
- Runtime duration
- Final pipeline status
- Failed step and failure reason, when applicable
- Command and step-level execution records
- Validation summary
- S3, Snowflake, and dbt execution mode when those stages are requested

A sanitized example is stored under:

```text
docs/sample_outputs/
```

### Warehouse lineage and monitoring

Lineage is preserved from RAW into dbt rather than being discarded after ingestion. The core lineage fields are:

```text
pipeline_run_id
source_file
source_row_number
loaded_at
```

The Gold layer also includes:

```text
gold_pipeline_batch_metadata
```

and the monitoring layer includes:

```text
monitoring_pipeline_row_counts
```

Together, these provide batch identity, source provenance, and lightweight row-count visibility for downstream inspection.

---

## Snowpipe POC

The repository includes Snowpipe SQL/configuration artifacts that illustrate an event-driven auto-ingest design. It is intentionally classified as a **configuration proof of concept**.

Intended pattern:

```text
S3 object-created event
→ Snowflake notification channel
→ Snowpipe COPY INTO
→ test RAW table
```

Relevant test objects include:

| Object | Purpose |
|---|---|
| `RAW_TRANSACTIONS_PIPE_TEST` | Test landing table |
| `PIPE_TRANSACTIONS_SNOWPIPE_TEST` | Pipe configured with `AUTO_INGEST = TRUE` |
| `raw/snowpipe_test/transactions/` | Dedicated test prefix |

The repository demonstrates the configuration pattern. **It does not claim independently verified live S3 event delivery or production Snowpipe auto-ingestion.**

---

## Streamlit Analytics

The dashboard is located at:

```text
dashboards/streamlit_app.py
```

It is designed to expose analytical outputs from the Gold and monitoring layers, including:

- Fraud KPIs by card network
- Daily fraud trends
- Dispute and chargeback KPIs
- Chargeback win/loss outcomes
- Pipeline row-count monitoring

Snowflake configuration is environment driven, including `SNOWFLAKE_DATABASE`, so the dashboard code does not need SQL edits to point at a different configured database.

Run locally after Snowflake is configured:

```powershell
streamlit run dashboards\streamlit_app.py
```

---

## Implemented vs POC

The project intentionally distinguishes repository implementation from live external execution and production operation.

| Capability | Status |
|---|---|
| Deterministic synthetic generation | **Implemented + tested** |
| Immutable raw snapshots and raw-manifest verification | **Implemented + tested** |
| Versioned V1/V2 data contracts | **Implemented + tested** |
| Stateful incremental selection + checkpoints | **Implemented + tested** |
| Recovery-aware incremental orchestration with checkpoint commit after downstream success | **Implemented + tested** |
| Sparse incremental partitioning + zero-change batches | **Implemented + tested** |
| Semantic validation, duplicate detection, referential/composite integrity | **Implemented + tested** |
| Severity-aware quarantine and failure handling | **Implemented + tested** |
| Run-scoped validated and incremental output + partitioning | **Implemented + tested** |
| Idempotent S3 publisher and completion-marker behavior | **Implemented + tested behavior; live mutation is opt-in** |
| Guarded V1 Snowflake full-reload loader | **Implemented + guardrail-tested; live execution requires a configured target** |
| Snowflake `MERGE` incremental warehouse loading | **Implemented + automated-test and dry-run verified; live execution requires a configured target** |
| 13 dbt model definitions and dbt tests | **Implemented** |
| Current successful live dbt build | **Not claimed** |
| Four-task Airflow local DAG | **Implemented for run ID → generate → validate → partition** |
| Airflow orchestration of S3 → Snowflake → dbt | **Not implemented** |
| GitHub Actions and Docker verification configuration | **Implemented** |
| Snowpipe auto-ingest | **Configuration POC; live auto-ingestion not claimed** |
| Production Airflow/cloud deployment | **Not claimed** |
| Grounded AI investigation layer | **Not implemented; next phase** |

This distinction is deliberate: having code for an integration, testing its behavior, executing against a configured external environment, and operating that integration in production are different claims.

---

## Repository Structure

```text
.
├── .github/
│   └── workflows/ci.yml
├── airflow/
│   ├── dags/fraud_dispute_pipeline_dag.py
│   ├── docker-compose.yml
│   └── README.md
├── contracts/
│   ├── v1/
│   └── v2/
├── dashboards/
│   └── streamlit_app.py
├── dbt/
│   └── fraud_dispute_dbt/
│       ├── models/
│       │   ├── bronze/
│       │   ├── silver/
│       │   ├── gold/
│       │   └── monitoring/
│       └── tests/
├── docs/
│   ├── data_contract_failure_policy.md
│   └── sample_outputs/
├── scripts/
│   ├── generate_data.py
│   ├── incremental_selection.py
│   ├── pipeline.py
│   ├── run_pipeline.py
│   ├── run_snowflake_sql.py
│   ├── upload_partitioned_to_s3.py
│   ├── validate_data_contracts.py
│   └── partition_data_for_s3.py
├── sql/
│   ├── snowflake_setup.sql
│   ├── setup_s3_stage_template.sql
│   ├── load_raw_from_s3.sql
│   ├── validate_raw_counts.sql
│   ├── setup_snowflake_role_template.sql
│   └── setup_snowpipe_template.sql
├── tests/
├── Dockerfile
├── requirements.lock.txt
└── requirements-dev.lock.txt
```

Key reusable Snowflake scripts:

| Script | Purpose |
|---|---|
| `sql/snowflake_setup.sql` | Creates core Snowflake objects and RAW landing tables |
| `sql/migrate_v2_lineage_columns.sql` | One-time migration for pre-V2 RAW lineage columns |
| `sql/setup_s3_stage_template.sql` | Template for storage integration and S3 external stage setup |
| `sql/load_raw_from_s3.sql` | Guarded V1 full-RAW loading and replacement |
| `sql/merge_raw_from_s3.sql` | Guarded V2 incremental RAW loading with primary-key and `updated_at`-based `MERGE` semantics |
| `sql/validate_raw_counts.sql` | RAW row-count validation queries |
| `sql/setup_snowflake_role_template.sql` | Role/grant setup template |
| `sql/setup_snowpipe_template.sql` | Snowpipe configuration POC |

---

## Production Considerations

This project is intentionally sized as a portfolio platform, so production hardening is treated as a set of architectural questions rather than a shopping list of additional tools.

At materially larger scale or under production operational requirements, the design would need decisions around:

- **Infrastructure lifecycle:** repeatable provisioning, environment isolation, ownership, and change control for cloud and warehouse resources
- **Secrets and identity:** managed credentials, least-privilege roles, key rotation, and workload identity
- **Ingestion state:** append/incremental semantics, file-level load state, late-arriving data, replay boundaries, and deduplication across batches
- **Warehouse promotion:** stronger deployment and rollback patterns for concurrent or continuously arriving workloads
- **Transformation strategy:** incremental model behavior where full rebuilds are no longer appropriate
- **Observability:** freshness, volume, quality, and run-failure alerting with operational ownership and escalation paths
- **Orchestration:** managed deployment, durable scheduling, backfills, notifications, concurrency controls, and service-level expectations
- **Access control:** environment-specific Snowflake roles and separation of operational duties

The guarded V1 full-reload path remains available for backward-compatible replay, while V2 is the primary stateful incremental path. Each approach is explicit about its tradeoffs and failure semantics.

---

## Next Phase: Grounded AI Investigation

The next project phase is a grounded fraud/dispute investigation layer built **on top of** the completed data-platform foundation. The goal is to answer investigation questions using structured platform data and curated supporting documents while preserving source attribution and evaluation boundaries.

That AI layer is **not implemented or claimed as part of the current data-platform release**.

---

## Disclaimer

This repository is a portfolio project using fully synthetic fraud, dispute, chargeback, customer, and transaction data.

It does not contain proprietary company data, real customer data, production credentials, secrets, or a claim of production operation. External-system execution requires explicit configuration and is dry-run by default where supported by the pipeline CLI.
