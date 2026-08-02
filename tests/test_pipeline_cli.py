from __future__ import annotations

import sys

import pytest

from scripts.pipeline import (
    DBT_PROJECT_DIR,
    PROJECT_ROOT,
    build_command,
    build_parser,
    parse_cli_args,
)


RUN_ID = "20260730T190213Z_540d0c90"


def test_parser_exposes_stage_oriented_commands():
    parser = build_parser()
    help_text = parser.format_help()

    for command in (
        "run",
        "validate",
        "partition",
        "upload-s3",
        "load-snowflake",
        "dbt",
    ):
        assert command in help_text


def test_run_command_preserves_existing_pipeline_entrypoint():
    args = parse_cli_args(["run"])
    command, cwd = build_command(args)

    assert command == [
        sys.executable,
        "scripts/run_pipeline.py",
    ]
    assert cwd == PROJECT_ROOT


def test_run_command_maps_clear_options_to_legacy_flags():
    args = parse_cli_args(
        [
            "run",
            "--run-id",
            RUN_ID,
            "--skip-generate",
            "--upload-s3",
            "--bucket",
            "example-bucket",
            "--execute-s3",
            "--load-snowflake",
            "--sql-file",
            "sql/custom_load.sql",
            "--execute-snowflake",
            "--dbt",
            "--dbt-target",
            "ci",
        ]
    )
    command, cwd = build_command(args)

    assert command == [
        sys.executable,
        "scripts/run_pipeline.py",
        "--run-id",
        RUN_ID,
        "--skip-generate",
        "--upload-s3",
        "--s3-bucket",
        "example-bucket",
        "--execute-s3-upload",
        "--reload-snowflake",
        "--snowflake-reload-sql",
        "sql/custom_load.sql",
        "--execute-snowflake-reload",
        "--run-dbt",
        "--dbt-target",
        "ci",
    ]
    assert cwd == PROJECT_ROOT




def test_run_rejects_skip_generate_without_run_id():
    with pytest.raises(SystemExit):
        parse_cli_args(
            [
                "run",
                "--skip-generate",
            ]
        )

def test_run_rejects_execute_s3_without_upload():
    with pytest.raises(SystemExit):
        parse_cli_args(["run", "--execute-s3"])


def test_validate_builds_one_stage_command():
    args = parse_cli_args(
        [
            "validate",
            "--run-id",
            RUN_ID,
        ]
    )
    command, cwd = build_command(args)

    assert command == [
        sys.executable,
        "scripts/validate_data_contracts.py",
        "--run-id",
        RUN_ID,
    ]
    assert cwd == PROJECT_ROOT


def test_partition_builds_one_stage_command():
    args = parse_cli_args(
        [
            "partition",
            "--run-id",
            RUN_ID,
        ]
    )
    command, cwd = build_command(args)

    assert command == [
        sys.executable,
        "scripts/partition_data_for_s3.py",
        "--run-id",
        RUN_ID,
    ]
    assert cwd == PROJECT_ROOT


def test_upload_s3_maps_idempotency_options():
    args = parse_cli_args(
        [
            "upload-s3",
            "--run-id",
            RUN_ID,
            "--bucket",
            "example-bucket",
            "--prefix",
            "landing/raw",
            "--execute",
            "--allow-overwrite",
        ]
    )
    command, cwd = build_command(args)

    assert command == [
        sys.executable,
        "scripts/upload_partitioned_to_s3.py",
        "--bucket",
        "example-bucket",
        "--run-id",
        RUN_ID,
        "--prefix",
        "landing/raw",
        "--execute",
        "--allow-overwrite",
    ]
    assert cwd == PROJECT_ROOT


def test_load_snowflake_and_dbt_build_commands():
    snowflake_args = parse_cli_args(
        [
            "load-snowflake",
            "--run-id",
            RUN_ID,
            "--execute",
        ]
    )
    snowflake_command, snowflake_cwd = build_command(
        snowflake_args
    )

    assert snowflake_command == [
        sys.executable,
        "scripts/run_snowflake_sql.py",
        "--sql-file",
        "sql/load_raw_from_s3.sql",
        "--run-id",
        RUN_ID,
        "--execute",
    ]
    assert snowflake_cwd == PROJECT_ROOT

    dbt_args = parse_cli_args(
        [
            "dbt",
            "--target",
            "ci",
        ]
    )
    dbt_command, dbt_cwd = build_command(dbt_args)

    assert dbt_command == [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        "build",
        "--target",
        "ci",
    ]
    assert dbt_cwd == DBT_PROJECT_DIR
