# Local Airflow Orchestration

This environment runs Apache Airflow separately from the pipeline runtime.

Airflow owns orchestration metadata, scheduling, retries, logs, and task
dependencies. Each data-processing stage runs in the existing
`fraud-dispute-pipeline:local` image through `DockerOperator`, preserving the
pipeline's hash-locked application dependencies.

## Implemented workflow

```text
create_pipeline_run_id
          |
          v
generate_synthetic_data
          |
          v
validate_data_contracts
          |
          v
partition_validated_data
```

One run ID is created at the beginning and passed to generation, validation,
and partitioning through XCom templating. Generated datasets are written to
`data/raw/<run_id>/` in the shared Docker volume. Dataset payloads are never
stored in XCom.

The DAG intentionally stops after local partitioning. S3 publication,
Snowflake loading, and dbt are available through `scripts/pipeline.py`, but
they are not currently Airflow tasks.

## Reliability settings

The local DAG includes:

- two retries per Docker task
- a one-minute retry delay
- ten-minute task execution timeouts
- a thirty-minute DAG-run timeout
- `max_active_runs=1`
- a shared run-scoped data volume

Run-scoped storage provides the data isolation. `max_active_runs=1` is an
additional local resource and scheduling safeguard, not the primary
concurrency control.

## Local-only security warning

This Compose environment is for learning and portfolio demonstrations. It:

- enables Airflow's all-admin local authentication mode
- mounts the Docker daemon socket into the scheduler
- uses development-only PostgreSQL credentials

Do not deploy this Compose file as a production environment.

## 1. Create the untracked local environment file

From the repository root in PowerShell:

```powershell
@'
import secrets
from pathlib import Path

content = (
    "AIRFLOW_UID=50000\n"
    f"AIRFLOW_JWT_SECRET={secrets.token_urlsafe(48)}\n"
)

Path("airflow/.env").write_text(
    content,
    encoding="utf-8",
    newline="\n",
)

print("Created airflow/.env")
'@ | python -
```

## 2. Build the pipeline and Airflow images

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  build
```

## 3. Initialize metadata and the shared data volume

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  up airflow-init
```

A successful initialization ends with `airflow-init` exiting with code `0`.

## 4. Start Airflow

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  up --detach
```

Inspect the services:

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  ps
```

Open `http://localhost:8080`.

The DAG starts paused. Unpause `fraud_dispute_analytics_pipeline`, trigger it
manually, and inspect the graph and task logs.

## 5. Verify DAG discovery and import health

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  exec airflow-dag-processor `
  airflow dags list
```

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  exec airflow-dag-processor `
  airflow dags list-import-errors
```

## 6. Inspect service logs

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  logs --follow airflow-scheduler
```

## 7. Stop without deleting data

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  down
```

## 8. Full local reset

This deletes Airflow metadata and the generated pipeline data volume:

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  down --volumes --remove-orphans
```
