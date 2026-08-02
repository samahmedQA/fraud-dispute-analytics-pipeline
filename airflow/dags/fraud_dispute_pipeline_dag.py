from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task


PIPELINE_IMAGE = os.getenv(
    "PIPELINE_IMAGE",
    "fraud-dispute-pipeline:local",
)
PIPELINE_DATA_VOLUME = os.getenv(
    "PIPELINE_DATA_VOLUME",
    "fraud-dispute-pipeline-data",
)

RUN_ID_XCOM = (
    "{{ ti.xcom_pull(task_ids='create_pipeline_run_id') }}"
)


def docker_task_settings() -> dict[str, object]:
    """Return isolated settings shared by pipeline container tasks."""
    return {
        "image": PIPELINE_IMAGE,
        "api_version": "auto",
        "docker_url": "unix://var/run/docker.sock",
        "mount_tmp_dir": False,
        "mounts": [
            {
                "source": PIPELINE_DATA_VOLUME,
                "target": "/app/data",
                "type": "volume",
            }
        ],
        "working_dir": "/app",
        "force_pull": False,
        "auto_remove": "success",
        "do_xcom_push": False,
    }


@dag(
    dag_id="fraud_dispute_analytics_pipeline",
    description=(
        "Generate, validate, and partition one tracked fraud-dispute "
        "pipeline batch."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=[
        "fraud",
        "disputes",
        "data-quality",
        "docker",
        "portfolio",
    ],
)
def fraud_dispute_analytics_pipeline() -> None:
    """
    Orchestrate local batch stages using the tested pipeline image.

    The Airflow task returns only the small run identifier through XCom.
    Generated datasets remain in a shared Docker volume rather than being
    transferred through Airflow metadata.
    """

    @task(task_id="create_pipeline_run_id")
    def create_pipeline_run_id() -> str:
        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        run_id = f"{timestamp}_{uuid.uuid4().hex[:8]}"

        print(f"Created pipeline run ID: {run_id}")
        return run_id

    pipeline_run_id = create_pipeline_run_id()

    generate_data = DockerOperator(
        task_id="generate_synthetic_data",
        entrypoint=[
            "python",
            "scripts/generate_data.py",
        ],
        command=[
            "--run-id",
            RUN_ID_XCOM,
        ],
        **docker_task_settings(),
    )

    validate_contracts = DockerOperator(
        task_id="validate_data_contracts",
        command=[
            "validate",
            "--run-id",
            RUN_ID_XCOM,
        ],
        **docker_task_settings(),
    )

    partition_data = DockerOperator(
        task_id="partition_validated_data",
        command=[
            "partition",
            "--run-id",
            RUN_ID_XCOM,
        ],
        **docker_task_settings(),
    )

    (
        pipeline_run_id
        >> generate_data
        >> validate_contracts
        >> partition_data
    )


fraud_dispute_analytics_pipeline()
