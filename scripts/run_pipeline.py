from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DIR = PROJECT_ROOT / "data" / "pipeline_audit_logs"
VALIDATION_REPORTS_DIR = PROJECT_ROOT / "data" / "validation_reports"


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def generate_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{timestamp}_{short_uuid}"


def safe_count(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, list):
        return len(value)

    if isinstance(value, dict):
        return sum(safe_count(item) for item in value.values())

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def load_validation_summary() -> dict[str, Any]:
    summary: dict[str, Any] = {
        "rows_received": 0,
        "rows_valid": 0,
        "rows_invalid": 0,
        "rows_quarantined": 0,
        "rows_rejected": 0,
        "warnings": 0,
        "datasets": [],
    }

    if not VALIDATION_REPORTS_DIR.exists():
        return summary

    for report_path in sorted(VALIDATION_REPORTS_DIR.glob("*_validation_report.json")):
        try:
            with report_path.open("r", encoding="utf-8") as file:
                report = json.load(file)
        except json.JSONDecodeError:
            continue

        dataset = report.get(
            "dataset",
            report_path.name.replace("_validation_report.json", ""),
        )

        status = report.get("status", report.get("batch_status", "UNKNOWN"))
        pipeline_action = report.get("pipeline_action", "UNKNOWN")

        total_records = safe_count(report.get("total_records"))
        invalid_records = safe_count(report.get("invalid_records"))

        valid_records = safe_count(report.get("valid_records"))
        if valid_records == 0 and total_records > 0:
            valid_records = max(total_records - invalid_records, 0)

        warning_count = safe_count(
            report.get(
                "warning_count",
                report.get("warnings", report.get("warning_records")),
            )
        )

        quarantined_records = 0
        rejected_records = 0

        if status == "PASSED_WITH_QUARANTINE" or pipeline_action == "UPLOAD_VALID_RECORDS_ONLY":
            quarantined_records = invalid_records

        if status == "FAILED" or pipeline_action == "BLOCK_S3_UPLOAD":
            rejected_records = invalid_records

        dataset_summary = {
            "dataset": dataset,
            "status": status,
            "pipeline_action": pipeline_action,
            "rows_received": total_records,
            "rows_valid": valid_records,
            "rows_invalid": invalid_records,
            "rows_quarantined": quarantined_records,
            "rows_rejected": rejected_records,
            "warnings": warning_count,
            "report_file": str(report_path.relative_to(PROJECT_ROOT)),
        }

        summary["datasets"].append(dataset_summary)
        summary["rows_received"] += total_records
        summary["rows_valid"] += valid_records
        summary["rows_invalid"] += invalid_records
        summary["rows_quarantined"] += quarantined_records
        summary["rows_rejected"] += rejected_records
        summary["warnings"] += warning_count

    return summary


def create_audit_record(args: argparse.Namespace) -> dict[str, Any]:
    run_id = generate_run_id()

    return {
        "run_id": run_id,
        "pipeline_name": "fraud_dispute_analytics_pipeline",
        "started_at_utc": utc_now(),
        "ended_at_utc": None,
        "duration_seconds": None,
        "status": "RUNNING",
        "failure_reason": None,
        "command": " ".join(sys.argv),
        "config": {
            "skip_generate": args.skip_generate,
            "upload_s3_requested": args.upload_s3,
            "s3_upload_mode": "execute" if args.execute_s3_upload else "dry_run",
            "s3_bucket": args.s3_bucket,
            "snowflake_reload_requested": args.reload_snowflake,
            "snowflake_reload_mode": "execute" if args.execute_snowflake_reload else "dry_run",
            "snowflake_reload_sql": args.snowflake_reload_sql,
            "run_dbt_requested": args.run_dbt,
            "dbt_target": args.dbt_target,
        },
        "steps": [],
        "validation_summary": {},
        "audit_file": str(Path("data") / "pipeline_audit_logs" / f"pipeline_run_{run_id}.json"),
    }


def write_audit_record(audit_record: dict[str, Any]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    audit_record["validation_summary"] = load_validation_summary()

    audit_file = PROJECT_ROOT / audit_record["audit_file"]
    audit_log_file = AUDIT_DIR / "pipeline_run_audit_log.jsonl"

    with audit_file.open("w", encoding="utf-8") as file:
        json.dump(audit_record, file, indent=2)

    with audit_log_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps(audit_record) + "\n")

    print(f"Pipeline audit file written: {audit_file}")


def run_step(
    step_name: str,
    command: list[str],
    audit_record: dict[str, Any],
) -> None:
    print()
    print("=" * 80)
    print(f"Running step: {step_name}")
    print("=" * 80)
    print(f"Command: {' '.join(command)}")

    step_started_at = utc_now()
    step_timer = time.perf_counter()

    step_record: dict[str, Any] = {
        "step_name": step_name,
        "command": " ".join(command),
        "started_at_utc": step_started_at,
        "ended_at_utc": None,
        "duration_seconds": None,
        "status": "RUNNING",
        "failure_reason": None,
    }

    audit_record["steps"].append(step_record)

    try:
        subprocess.run(command, cwd=PROJECT_ROOT, check=True)

        step_record["status"] = "SUCCESS"
        print(f"Step completed successfully: {step_name}")

    except subprocess.CalledProcessError as error:
        failure_reason = f"Command failed with exit code {error.returncode}"

        step_record["status"] = "FAILED"
        step_record["failure_reason"] = failure_reason

        audit_record["status"] = "FAILED"
        audit_record["failure_reason"] = f"{step_name}: {failure_reason}"

        print(f"Step failed: {step_name}")
        print(failure_reason)

        raise

    finally:
        step_record["ended_at_utc"] = utc_now()
        step_record["duration_seconds"] = round(time.perf_counter() - step_timer, 2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local fraud dispute analytics pipeline."
    )

    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip synthetic data generation and use existing files in data/raw.",
    )

    parser.add_argument(
        "--run-dbt",
        action="store_true",
        help="Run dbt build after local validation and partitioning.",
    )

    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Run the S3 upload step after partitioning.",
    )

    parser.add_argument(
        "--s3-bucket",
        default="fraud-dispute-analytics-sam-23402",
        help="Target S3 bucket for partitioned raw files.",
    )

    parser.add_argument(
        "--execute-s3-upload",
        action="store_true",
        help="Actually upload files to S3. Without this flag, upload runs as dry run.",
    )

    parser.add_argument(
        "--reload-snowflake",
        action="store_true",
        help="Run the Snowflake RAW reload SQL step after S3 upload or partitioning.",
    )

    parser.add_argument(
        "--snowflake-reload-sql",
        default="sql/load_raw_from_s3.sql",
        help="SQL file used for Snowflake RAW reload. Default: sql/load_raw_from_s3.sql.",
    )

    parser.add_argument(
        "--execute-snowflake-reload",
        action="store_true",
        help="Actually execute Snowflake reload SQL. Without this flag, reload runs as dry run.",
    )

    parser.add_argument(
        "--dbt-target",
        default="dev",
        help="dbt target to use when --run-dbt is provided. Default: dev.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit_record = create_audit_record(args)
    pipeline_timer = time.perf_counter()

    print("Starting local fraud dispute analytics pipeline")
    print(f"Pipeline Run ID: {audit_record['run_id']}")

    try:
        if not args.skip_generate:
            run_step(
                step_name="Generate synthetic data",
                command=[sys.executable, "scripts/generate_data.py"],
                audit_record=audit_record,
            )

        run_step(
            step_name="Validate data contracts",
            command=[sys.executable, "scripts/validate_data_contracts.py"],
            audit_record=audit_record,
        )

        run_step(
            step_name="Partition raw data for S3",
            command=[sys.executable, "scripts/partition_data_for_s3.py"],
            audit_record=audit_record,
        )

        if args.upload_s3:
            s3_command = [
                sys.executable,
                "scripts/upload_partitioned_to_s3.py",
                "--bucket",
                args.s3_bucket,
            ]

            if args.execute_s3_upload:
                s3_command.append("--execute")

            run_step(
                step_name="Upload partitioned raw files to S3",
                command=s3_command,
                audit_record=audit_record,
            )

        if args.reload_snowflake:
            snowflake_command = [
                sys.executable,
                "scripts/run_snowflake_sql.py",
                "--sql-file",
                args.snowflake_reload_sql,
            ]

            if args.execute_snowflake_reload:
                snowflake_command.append("--execute")

            run_step(
                step_name="Reload Snowflake RAW tables from S3",
                command=snowflake_command,
                audit_record=audit_record,
            )

        if args.run_dbt:
            run_step(
                step_name=f"Run dbt build against target: {args.dbt_target}",
                command=[
                    sys.executable,
                    "-c",
                    "from dbt.cli.main import cli; cli()",
                    "build",
                    "--target",
                    args.dbt_target,
                ],
                audit_record=audit_record,
            )

        audit_record["status"] = "SUCCESS"
        print()
        print("Pipeline completed successfully.")

    except subprocess.CalledProcessError:
        print()
        print("Pipeline failed.")
        raise

    finally:
        audit_record["ended_at_utc"] = utc_now()
        audit_record["duration_seconds"] = round(time.perf_counter() - pipeline_timer, 2)
        write_audit_record(audit_record)


if __name__ == "__main__":
    main()
