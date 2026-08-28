from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_pipeline.py"

SPEC = importlib.util.spec_from_file_location(
    "run_pipeline_recovery",
    SCRIPT_PATH,
)
assert SPEC is not None and SPEC.loader is not None

pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)

RUN_ID = "20260828T190000Z_aaaaaaaa"


def make_args(
    *,
    mode: str = "incremental",
    execute_s3: bool = True,
    execute_snowflake: bool = True,
    run_dbt: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        run_id=RUN_ID,
        skip_generate=True,
        mode=mode,
        run_dbt=run_dbt,
        upload_s3=True,
        s3_bucket="test-bucket",
        execute_s3_upload=execute_s3,
        reload_snowflake=True,
        snowflake_reload_sql=None,
        execute_snowflake_reload=execute_snowflake,
        dbt_target="dev",
    )


def install_test_harness(
    monkeypatch: pytest.MonkeyPatch,
    args: argparse.Namespace,
    *,
    fail_step: str | None = None,
) -> list[tuple[str, list[str]]]:
    calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(
        pipeline,
        "parse_args",
        lambda: args,
    )

    monkeypatch.setattr(
        pipeline,
        "write_audit_record",
        lambda audit_record: None,
    )

    def fake_run_step(
        step_name: str,
        command: list[str],
        audit_record: dict,
        cwd: Path = pipeline.PROJECT_ROOT,
    ) -> None:
        calls.append((step_name, command))

        if step_name == fail_step:
            audit_record["status"] = "FAILED"
            audit_record["failure_reason"] = (
                f"{step_name}: simulated failure"
            )
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=command,
            )

    monkeypatch.setattr(
        pipeline,
        "run_step",
        fake_run_step,
    )

    return calls


def test_incremental_success_commits_checkpoint_last(
    monkeypatch: pytest.MonkeyPatch,
):
    args = make_args()
    calls = install_test_harness(
        monkeypatch,
        args,
    )

    pipeline.main()

    step_names = [
        step_name
        for step_name, _ in calls
    ]

    assert step_names == [
        "Validate data contracts",
        "Select incremental changes",
        "Partition incremental data for S3",
        "Upload partitioned raw files to S3",
        "Merge incremental Snowflake RAW tables from S3",
        "Commit incremental checkpoint",
    ]

    partition_command = calls[2][1]
    assert partition_command[-2:] == [
        "--source",
        "incremental",
    ]

    snowflake_command = calls[4][1]
    assert "sql/merge_raw_from_s3.sql" in snowflake_command
    assert snowflake_command[-3:] == [
        "--mode",
        "incremental",
        "--execute",
    ]

    commit_command = calls[-1][1]
    assert commit_command == [
        sys.executable,
        "scripts/incremental_selection.py",
        "commit",
        "--run-id",
        RUN_ID,
    ]


def test_snowflake_failure_never_commits_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
):
    args = make_args()

    calls = install_test_harness(
        monkeypatch,
        args,
        fail_step=(
            "Merge incremental Snowflake RAW tables from S3"
        ),
    )

    with pytest.raises(subprocess.CalledProcessError):
        pipeline.main()

    step_names = [
        step_name
        for step_name, _ in calls
    ]

    assert (
        "Merge incremental Snowflake RAW tables from S3"
        in step_names
    )
    assert "Commit incremental checkpoint" not in step_names


def test_incremental_dry_run_leaves_candidate_uncommitted(
    monkeypatch: pytest.MonkeyPatch,
):
    args = make_args(
        execute_s3=False,
        execute_snowflake=False,
    )

    calls = install_test_harness(
        monkeypatch,
        args,
    )

    pipeline.main()

    step_names = [
        step_name
        for step_name, _ in calls
    ]

    assert "Select incremental changes" in step_names
    assert "Partition incremental data for S3" in step_names
    assert (
        "Merge incremental Snowflake RAW tables from S3"
        in step_names
    )
    assert "Commit incremental checkpoint" not in step_names


def test_requested_dbt_must_succeed_before_checkpoint_commit(
    monkeypatch: pytest.MonkeyPatch,
):
    args = make_args(
        run_dbt=True,
    )

    calls = install_test_harness(
        monkeypatch,
        args,
        fail_step="Run dbt build against target: dev",
    )

    with pytest.raises(subprocess.CalledProcessError):
        pipeline.main()

    step_names = [
        step_name
        for step_name, _ in calls
    ]

    assert "Run dbt build against target: dev" in step_names
    assert "Commit incremental checkpoint" not in step_names


def test_full_mode_preserves_full_partition_and_reload(
    monkeypatch: pytest.MonkeyPatch,
):
    args = make_args(
        mode="full",
    )

    calls = install_test_harness(
        monkeypatch,
        args,
    )

    pipeline.main()

    step_names = [
        step_name
        for step_name, _ in calls
    ]

    assert "Select incremental changes" not in step_names
    assert "Commit incremental checkpoint" not in step_names
    assert "Partition validated data for S3" in step_names
    assert "Reload Snowflake RAW tables from S3" in step_names

    partition = next(
        command
        for name, command in calls
        if name == "Partition validated data for S3"
    )
    assert "--source" not in partition

    snowflake = next(
        command
        for name, command in calls
        if name == "Reload Snowflake RAW tables from S3"
    )

    assert "sql/load_raw_from_s3.sql" in snowflake
    assert "--mode" not in snowflake
