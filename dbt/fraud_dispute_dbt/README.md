# Fraud & Dispute Analytics dbt Project

This dbt project transforms Snowflake RAW fraud and dispute data into tested
bronze, silver, gold, and monitoring models.

## Model layers

- `bronze/` flattens RAW JSON and preserves `pipeline_run_id`, `source_file`,
  `source_row_number`, and `loaded_at`.
- `silver/` applies typed business transformations while retaining batch
  lineage.
- `gold/` provides daily and network-level fraud, dispute, and chargeback
  metrics.
- `gold_pipeline_batch_metadata` exposes batch-level lineage for aggregate
  reporting and future AI evidence tracking.
- `monitoring/` contains pipeline row-count checks.

## Configure the profile

Copy the example profile outside the repository:

```powershell
Copy-Item `
  profiles.yml.example `
  $HOME\.dbt\profiles.yml
```

Update the copied profile with the intended Snowflake account, role,
warehouse, database, schema, and authentication values. Never commit a real
profile containing credentials.

## Run the project

From `dbt/fraud_dispute_dbt`:

```powershell
python -c "from dbt.cli.main import cli; cli()" build
```

Select a target explicitly:

```powershell
python -c "from dbt.cli.main import cli; cli()" `
  build `
  --target dev
```

The repository-level stage CLI provides the same build entry point:

```powershell
python scripts/pipeline.py dbt `
  --target dev
```

## Lineage tests

Singular dbt tests verify that lineage remains populated across bronze,
silver, and gold batch metadata:

```text
tests/assert_bronze_lineage_not_null.sql
tests/assert_silver_lineage_not_null.sql
tests/assert_gold_batch_lineage_not_null.sql
```

The Python suite also statically checks lineage propagation in the dbt model
definitions.
