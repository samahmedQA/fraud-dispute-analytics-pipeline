import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_command(args, expect_success=True):
    result = subprocess.run(
        [sys.executable, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if expect_success:
        assert result.returncode == 0, (
            f"Command failed: {' '.join(args)}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return result


def count_jsonl_lines(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def read_partitioned_dataset(dataset_name):
    dataset_dir = PROJECT_ROOT / "data" / "s3_partitioned" / "raw" / dataset_name

    if not dataset_dir.exists():
        return ""

    contents = []

    for file_path in dataset_dir.rglob("*.json"):
        contents.append(file_path.read_text(encoding="utf-8"))

    return "\n".join(contents)


def count_partitioned_dataset_lines(dataset_name):
    dataset_dir = PROJECT_ROOT / "data" / "s3_partitioned" / "raw" / dataset_name

    if not dataset_dir.exists():
        return 0

    total_lines = 0

    for file_path in dataset_dir.rglob("*.json"):
        total_lines += count_jsonl_lines(file_path)

    return total_lines


def test_normal_generated_data_validates_and_writes_validated_outputs():
    run_command(["scripts/generate_data.py"])
    result = run_command(["scripts/validate_data_contracts.py"])

    assert result.stdout.count("Status: PASSED") == 5
    assert "Invalid Records: 0" in result.stdout

    expected_counts = {
        "customers": 1500,
        "transactions": 10000,
        "fraud_signals": 10000,
        "disputes": 1200,
        "chargeback_outcomes": 840,
    }

    for dataset_name, expected_count in expected_counts.items():
        validated_file = PROJECT_ROOT / "data" / "validated" / f"{dataset_name}.json"

        assert validated_file.exists()
        assert count_jsonl_lines(validated_file) == expected_count


def test_hard_fail_blocks_schema_invalid_transactions():
    result = run_command(
        [
            "scripts/validate_data_contracts.py",
            "--dataset",
            "transactions",
            "--input-file",
            "tests/fixtures/bad_transactions.json",
        ],
        expect_success=False,
    )

    assert result.returncode == 1
    assert "Status: FAILED" in result.stdout
    assert "Pipeline Action: BLOCK_S3_UPLOAD" in result.stdout


def test_quarantine_path_partitions_only_valid_records():
    run_command(["scripts/generate_data.py"])
    run_command(["scripts/validate_data_contracts.py"])

    result = run_command(
        [
            "scripts/validate_data_contracts.py",
            "--dataset",
            "chargeback_outcomes",
            "--input-file",
            "tests/fixtures/bad_chargeback_outcomes.json",
        ]
    )

    assert "Status: PASSED_WITH_QUARANTINE" in result.stdout
    assert "Pipeline Action: UPLOAD_VALID_RECORDS_ONLY" in result.stdout
    assert "Invalid Records: 1" in result.stdout

    validated_file = PROJECT_ROOT / "data" / "validated" / "chargeback_outcomes.json"

    assert validated_file.exists()
    assert count_jsonl_lines(validated_file) == 1

    run_command(["scripts/partition_data_for_s3.py"])

    partitioned_contents = read_partitioned_dataset("chargeback_outcomes")

    assert "DISP_9999999" not in partitioned_contents
    assert count_partitioned_dataset_lines("chargeback_outcomes") == 1


def test_partitioning_reads_validated_data_not_raw_data():
    run_command(["scripts/generate_data.py"])
    run_command(["scripts/validate_data_contracts.py"])

    raw_chargebacks_file = PROJECT_ROOT / "data" / "raw" / "chargeback_outcomes.json"

    raw_only_sentinel_record = (
        '{"chargeback_id": "CB_SENTINEL_RAW_ONLY", '
        '"dispute_id": "DISP_SENTINEL_RAW_ONLY", '
        '"resolved_date": "2026-01-01 00:00:00"}\n'
    )

    with raw_chargebacks_file.open("a", encoding="utf-8") as file:
        file.write(raw_only_sentinel_record)

    run_command(["scripts/partition_data_for_s3.py"])

    partitioned_contents = read_partitioned_dataset("chargeback_outcomes")

    assert "CB_SENTINEL_RAW_ONLY" not in partitioned_contents
    assert "DISP_SENTINEL_RAW_ONLY" not in partitioned_contents
    assert count_partitioned_dataset_lines("chargeback_outcomes") == 840