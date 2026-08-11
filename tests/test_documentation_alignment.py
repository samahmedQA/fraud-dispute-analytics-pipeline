from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
README = PROJECT_ROOT / "README.md"
AIRFLOW_README = PROJECT_ROOT / "airflow" / "README.md"
DBT_README = (
    PROJECT_ROOT
    / "dbt"
    / "fraud_dispute_dbt"
    / "README.md"
)
FAILURE_POLICY = (
    PROJECT_ROOT
    / "docs"
    / "data_contract_failure_policy.md"
)
SAMPLE_AUDIT = (
    PROJECT_ROOT
    / "docs"
    / "sample_outputs"
    / "pipeline_run_audit_example.json"
)


def test_readme_uses_current_cli_and_airflow_scope():
    text = README.read_text(encoding="utf-8")

    for expected in (
        "python scripts/pipeline.py validate",
        "python scripts/pipeline.py partition",
        "python scripts/pipeline.py upload-s3",
        "python scripts/pipeline.py load-snowflake",
        "python scripts/pipeline.py run",
        "data/raw/<run_id>/",
        "data/validation_reports/<run_id>/",
    ):
        assert expected in text

    for outdated in (
        "preview_s3_upload",
        "preview_snowflake_raw_reload",
        "run_dbt_build",
        "python scripts/validate_data_contracts.py\n",
        "python scripts/partition_data_for_s3.py\n",
        "python scripts/upload_partitioned_to_s3.py --bucket",
    ):
        assert outdated not in text


def test_airflow_readme_matches_implemented_graph():
    text = AIRFLOW_README.read_text(
        encoding="utf-8"
    )

    for task_id in (
        "create_pipeline_run_id",
        "generate_synthetic_data",
        "validate_data_contracts",
        "partition_validated_data",
    ):
        assert task_id in text

    assert "not currently Airflow tasks" in text
    assert "preview_s3_upload" not in text
    assert "run_dbt_build" not in text


def test_project_specific_dbt_and_markdown_docs():
    dbt_text = DBT_README.read_text(
        encoding="utf-8"
    )
    policy_text = FAILURE_POLICY.read_text(
        encoding="utf-8"
    )

    assert "Welcome to your new dbt project!" not in dbt_text
    assert "pipeline_run_id" in dbt_text
    assert "\\# " not in policy_text
    assert "\\## " not in policy_text


def test_sample_audit_commands_are_run_scoped():
    audit = json.loads(
        SAMPLE_AUDIT.read_text(
            encoding="utf-8"
        )
    )
    run_id = audit["run_id"]

    commands = {
        step["step_name"]: step["command"]
        for step in audit["steps"]
    }

    validate_command = commands[
        "Validate data contracts"
    ]
    partition_command = commands[
        "Partition validated data for S3"
    ]

    assert "--run-id" in validate_command
    assert run_id in validate_command
    assert "--run-id" in partition_command
    assert run_id in partition_command

    for dataset in audit[
        "validation_summary"
    ]["datasets"]:
        report_file = dataset["report_file"]
        assert run_id in report_file
