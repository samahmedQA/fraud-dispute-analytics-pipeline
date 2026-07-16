from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "fraud_dispute_dbt"

S3_BUCKET = os.getenv("FRAUD_DISPUTE_S3_BUCKET", "fraud-dispute-analytics-sam-23402")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")


def run_command(command: list[str], cwd: Path = PROJECT_ROOT) -> None:
    """
    Run a project command from the repository root.

    This keeps the Airflow DAG focused on orchestration while the existing
    project scripts handle the actual data engineering logic.
    """
    print(f"Running command: {' '.join(command)}")
    print(f"Working directory: {cwd}")

    subprocess.run(
        command,
        cwd=cwd,
        check=True,
    )


def generate_synthetic_data() -> None:
    run_command([sys.executable, "scripts/generate_data.py"])


def validate_data_contracts() -> None:
    run_command([sys.executable, "scripts/validate_data_contracts.py"])


def partition_raw_data_for_s3() -> None:
    run_command([sys.executable, "scripts/partition_data_for_s3.py"])


def preview_s3_upload() -> None:
    run_command(
        [
            sys.executable,
            "scripts/upload_partitioned_to_s3.py",
            "--bucket",
            S3_BUCKET,
        ]
    )


def preview_snowflake_raw_reload() -> None:
    run_command(
        [
            sys.executable,
            "scripts/run_snowflake_sql.py",
            "--sql-file",
            "sql/load_raw_from_s3.sql",
        ]
    )


def run_dbt_build() -> None:
    run_command(
        [
            sys.executable,
            "-c",
            "from dbt.cli.main import cli; cli()",
            "build",
            "--target",
            DBT_TARGET,
        ],
        cwd=DBT_PROJECT_DIR,
    )


with DAG(
    dag_id="fraud_dispute_analytics_pipeline",
    description="Orchestrates the fraud dispute analytics pipeline from raw data validation through dbt marts.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fraud", "disputes", "snowflake", "dbt", "portfolio"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_synthetic_data",
        python_callable=generate_synthetic_data,
    )

    validate_contracts = PythonOperator(
        task_id="validate_data_contracts",
        python_callable=validate_data_contracts,
    )

    partition_for_s3 = PythonOperator(
        task_id="partition_raw_data_for_s3",
        python_callable=partition_raw_data_for_s3,
    )

    s3_upload_dry_run = PythonOperator(
        task_id="preview_s3_upload",
        python_callable=preview_s3_upload,
    )

    snowflake_reload_dry_run = PythonOperator(
        task_id="preview_snowflake_raw_reload",
        python_callable=preview_snowflake_raw_reload,
    )

    dbt_build = PythonOperator(
        task_id="run_dbt_build",
        python_callable=run_dbt_build,
    )

    (
        generate_data
        >> validate_contracts
        >> partition_for_s3
        >> s3_upload_dry_run
        >> snowflake_reload_dry_run
        >> dbt_build
    )
