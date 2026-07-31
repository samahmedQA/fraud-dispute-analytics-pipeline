# Local Airflow Orchestration

This environment runs Apache Airflow separately from the pipeline runtime.

Airflow owns orchestration metadata, scheduling, retries, logs, and task
dependencies. Each data-processing stage runs in the existing
`fraud-dispute-pipeline:local` image through `DockerOperator`, preserving the
pipeline's hash-locked application dependencies.

## Workflow

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

The same run ID is passed to validation and partitioning through Airflow XCom.
Dataset files are not stored in XCom. They remain in the shared named Docker
volume `fraud-dispute-pipeline-data`.

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

## 3. Initialize the metadata database and shared data volume

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

## 5. Verify DAG discovery

```powershell
docker compose `
  --env-file airflow/.env `
  --file airflow/docker-compose.yml `
  exec airflow-dag-processor `
  airflow dags list
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
