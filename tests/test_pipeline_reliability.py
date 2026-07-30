from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_ID_REGEX = re.compile(
    r"\b\d{8}T\d{6}Z_[0-9a-f]{8}\b"
)

HARD_FAIL_RUN_ID = (
    "20260729T210001Z_a1b2c3d4"
)

QUARANTINE_RUN_ID = (
    "20260729T210002Z_b2c3d4e5"
)

VALIDATED_ONLY_RUN_ID = (
    "20260729T210003Z_c3d4e5f6"
)

MALFORMED_RUN_ID = (
    "20260729T210004Z_d4e5f6a7"
)


def run_command(
    args: list[str],
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    if expect_success:
        assert result.returncode == 0, (
            f"Command failed: {' '.join(args)}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return result


def count_jsonl_lines(
    file_path: Path,
) -> int:
    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return sum(
            1
            for line in file
            if line.strip()
        )


def validated_file_path(
    run_id: str,
    dataset_name: str,
) -> Path:
    return (
        PROJECT_ROOT
        / "data"
        / "validated"
        / run_id
        / f"{dataset_name}.json"
    )


def validation_report_path(
    run_id: str,
    dataset_name: str,
) -> Path:
    return (
        PROJECT_ROOT
        / "data"
        / "validation_reports"
        / run_id
        / (
            f"{dataset_name}"
            "_validation_report.json"
        )
    )


def partitioned_dataset_dir(
    run_id: str,
    dataset_name: str,
) -> Path:
    return (
        PROJECT_ROOT
        / "data"
        / "s3_partitioned"
        / run_id
        / "raw"
        / dataset_name
    )


def read_partitioned_dataset(
    run_id: str,
    dataset_name: str,
) -> str:
    dataset_dir = partitioned_dataset_dir(
        run_id,
        dataset_name,
    )

    if not dataset_dir.exists():
        return ""

    contents: list[str] = []

    for file_path in sorted(
        dataset_dir.rglob("*.json")
    ):
        contents.append(
            file_path.read_text(
                encoding="utf-8"
            )
        )

    return "\n".join(contents)


def count_partitioned_dataset_lines(
    run_id: str,
    dataset_name: str,
) -> int:
    dataset_dir = partitioned_dataset_dir(
        run_id,
        dataset_name,
    )

    if not dataset_dir.exists():
        return 0

    return sum(
        count_jsonl_lines(file_path)
        for file_path
        in dataset_dir.rglob("*.json")
    )


def test_pipeline_propagates_run_id_and_writes_outputs():
    result = run_command(
        ["scripts/run_pipeline.py"]
    )

    run_ids = set(
        RUN_ID_REGEX.findall(
            result.stdout
        )
    )

    assert len(run_ids) == 1, (
        "Expected one run ID to be propagated "
        "through the pipeline, but found: "
        f"{sorted(run_ids)}\n"
        f"STDOUT:\n{result.stdout}"
    )

    run_id = run_ids.pop()

    dataset_status_lines = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() == "Status: PASSED"
    ]

    assert len(dataset_status_lines) == 5

    assert (
        "Invalid Records: 0"
        in result.stdout
    )

    expected_counts = {
        "customers": 1500,
        "transactions": 10000,
        "fraud_signals": 10000,
        "disputes": 1200,
        "chargeback_outcomes": 840,
    }

    for (
        dataset_name,
        expected_count,
    ) in expected_counts.items():
        validated_file = (
            validated_file_path(
                run_id,
                dataset_name,
            )
        )

        report_file = (
            validation_report_path(
                run_id,
                dataset_name,
            )
        )

        assert validated_file.exists()
        assert report_file.exists()

        assert (
            count_jsonl_lines(
                validated_file
            )
            == expected_count
        )

        report = json.loads(
            report_file.read_text(
                encoding="utf-8"
            )
        )

        assert report["run_id"] == run_id

        assert (
            report["batch_status"]
            == "PASSED"
        )

    manifest_file = (
        PROJECT_ROOT
        / "data"
        / "s3_partitioned"
        / run_id
        / "partition_manifest.json"
    )

    assert manifest_file.exists()

    manifest = json.loads(
        manifest_file.read_text(
            encoding="utf-8"
        )
    )

    assert manifest["run_id"] == run_id
    assert manifest["dataset_count"] == 5

    assert (
        manifest["records_received"]
        == sum(expected_counts.values())
    )

    assert (
        manifest["records_partitioned"]
        == sum(expected_counts.values())
    )

    audit_file = (
        PROJECT_ROOT
        / "data"
        / "pipeline_audit_logs"
        / f"pipeline_run_{run_id}.json"
    )

    assert audit_file.exists()

    audit = json.loads(
        audit_file.read_text(
            encoding="utf-8"
        )
    )

    assert audit["run_id"] == run_id
    assert audit["status"] == "SUCCESS"

    assert (
        audit["validation_summary"]
        ["rows_received"]
        == sum(expected_counts.values())
    )


def test_hard_fail_blocks_schema_invalid_transactions():
    result = run_command(
        [
            "scripts/validate_data_contracts.py",
            "--run-id",
            HARD_FAIL_RUN_ID,
            "--dataset",
            "transactions",
            "--input-file",
            (
                "tests/fixtures/"
                "bad_transactions.json"
            ),
        ],
        expect_success=False,
    )

    assert result.returncode == 1
    assert "Status: FAILED" in result.stdout

    assert (
        "Pipeline Action: BLOCK_S3_UPLOAD"
        in result.stdout
    )

    report_file = validation_report_path(
        HARD_FAIL_RUN_ID,
        "transactions",
    )

    assert report_file.exists()

    report = json.loads(
        report_file.read_text(
            encoding="utf-8"
        )
    )

    assert (
        report["run_id"]
        == HARD_FAIL_RUN_ID
    )

    assert (
        report["batch_status"]
        == "FAILED"
    )

    assert (
        report["pipeline_action"]
        == "BLOCK_S3_UPLOAD"
    )

    assert report["validated_file"] is None

    validated_file = validated_file_path(
        HARD_FAIL_RUN_ID,
        "transactions",
    )

    assert not validated_file.exists()


def test_quarantine_path_partitions_only_valid_records():
    run_command(
        ["scripts/generate_data.py"]
    )

    raw_chargebacks_file = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "chargeback_outcomes.json"
    )

    fixture_file = (
        PROJECT_ROOT
        / "tests"
        / "fixtures"
        / "bad_chargeback_outcomes.json"
    )

    original_raw_data = (
        raw_chargebacks_file.read_bytes()
    )

    try:
        raw_chargebacks_file.write_bytes(
            fixture_file.read_bytes()
        )

        result = run_command(
            [
                "scripts/"
                "validate_data_contracts.py",
                "--run-id",
                QUARANTINE_RUN_ID,
            ]
        )

        assert (
            "Status: "
            "PASSED_WITH_QUARANTINE"
            in result.stdout
        )

        assert (
            "Pipeline Action: "
            "UPLOAD_VALID_RECORDS_ONLY"
            in result.stdout
        )

        assert (
            "Invalid Records: 1"
            in result.stdout
        )

        validated_file = (
            validated_file_path(
                QUARANTINE_RUN_ID,
                "chargeback_outcomes",
            )
        )

        assert validated_file.exists()

        assert (
            count_jsonl_lines(
                validated_file
            )
            == 1
        )

        run_command(
            [
                "scripts/"
                "partition_data_for_s3.py",
                "--run-id",
                QUARANTINE_RUN_ID,
            ]
        )

        partitioned_contents = (
            read_partitioned_dataset(
                QUARANTINE_RUN_ID,
                "chargeback_outcomes",
            )
        )

        assert (
            "DISP_9999999"
            not in partitioned_contents
        )

        assert (
            count_partitioned_dataset_lines(
                QUARANTINE_RUN_ID,
                "chargeback_outcomes",
            )
            == 1
        )

    finally:
        raw_chargebacks_file.write_bytes(
            original_raw_data
        )


def test_partitioning_reads_validated_data_not_raw_data():
    run_command(
        ["scripts/generate_data.py"]
    )

    run_command(
        [
            "scripts/"
            "validate_data_contracts.py",
            "--run-id",
            VALIDATED_ONLY_RUN_ID,
        ]
    )

    raw_chargebacks_file = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "chargeback_outcomes.json"
    )

    original_raw_data = (
        raw_chargebacks_file.read_bytes()
    )

    sentinel_record = (
        '{"chargeback_id": '
        '"CB_SENTINEL_RAW_ONLY", '
        '"dispute_id": '
        '"DISP_SENTINEL_RAW_ONLY", '
        '"resolved_date": '
        '"2026-01-01 00:00:00"}\n'
    )

    try:
        with raw_chargebacks_file.open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                sentinel_record
            )

        run_command(
            [
                "scripts/"
                "partition_data_for_s3.py",
                "--run-id",
                VALIDATED_ONLY_RUN_ID,
            ]
        )

        partitioned_contents = (
            read_partitioned_dataset(
                VALIDATED_ONLY_RUN_ID,
                "chargeback_outcomes",
            )
        )

        assert (
            "CB_SENTINEL_RAW_ONLY"
            not in partitioned_contents
        )

        assert (
            "DISP_SENTINEL_RAW_ONLY"
            not in partitioned_contents
        )

        assert (
            count_partitioned_dataset_lines(
                VALIDATED_ONLY_RUN_ID,
                "chargeback_outcomes",
            )
            == 840
        )

    finally:
        raw_chargebacks_file.write_bytes(
            original_raw_data
        )


def test_malformed_json_hard_fails_with_validation_report():
    result = run_command(
        [
            "scripts/validate_data_contracts.py",
            "--run-id",
            MALFORMED_RUN_ID,
            "--dataset",
            "transactions",
            "--input-file",
            (
                "tests/fixtures/"
                "malformed_transactions.json"
            ),
        ],
        expect_success=False,
    )

    assert result.returncode == 1
    assert "Status: FAILED" in result.stdout

    assert (
        "Pipeline Action: BLOCK_S3_UPLOAD"
        in result.stdout
    )

    assert (
        "Malformed JSON Lines: 1"
        in result.stdout
    )

    report_file = validation_report_path(
        MALFORMED_RUN_ID,
        "transactions",
    )

    assert report_file.exists()

    report = json.loads(
        report_file.read_text(
            encoding="utf-8"
        )
    )

    assert (
        report["run_id"]
        == MALFORMED_RUN_ID
    )

    assert (
        report["batch_status"]
        == "FAILED"
    )

    assert (
        report["pipeline_action"]
        == "BLOCK_S3_UPLOAD"
    )

    assert report["validated_file"] is None

    assert (
        report["failed_rules"][0]
        ["rule_name"]
        == "valid_json_lines"
    )

    assert (
        report["failed_rules"][0]
        ["severity"]
        == "hard_fail"
    )

    assert (
        report["failed_rules"][0]
        ["line_number"]
        == 2
    )