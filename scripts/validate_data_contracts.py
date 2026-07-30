from __future__ import annotations

import argparse
import json
import re
import shutil
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
VALIDATED_DATA_ROOT = PROJECT_ROOT / "data" / "validated"
CONTRACTS_DIR = PROJECT_ROOT / "contracts" / "v1"
QUARANTINE_ROOT = PROJECT_ROOT / "data" / "quarantine"
REPORTS_ROOT = PROJECT_ROOT / "data" / "validation_reports"

CONTRACT_VERSION = "v1"

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
)


REFERENCE_RULES = {
    "chargeback_outcomes": [
        {
            "child_field": "dispute_id",
            "parent_dataset": "disputes",
            "parent_file": RAW_DATA_DIR / "disputes.json",
            "parent_key": "dispute_id",
        }
    ]
}


DATASETS = {
    "chargeback_outcomes": {
        "raw_file": (
            RAW_DATA_DIR
            / "chargeback_outcomes.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "chargeback_outcomes.schema.json"
        ),
        "primary_key": "chargeback_id",
        "timestamp_field": "resolved_date",
    },
    "customers": {
        "raw_file": (
            RAW_DATA_DIR
            / "customers.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "customers.schema.json"
        ),
        "primary_key": "customer_id",
        "timestamp_field": "created_at",
    },
    "disputes": {
        "raw_file": (
            RAW_DATA_DIR
            / "disputes.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "disputes.schema.json"
        ),
        "primary_key": "dispute_id",
        "timestamp_field": "opened_date",
    },
    "fraud_signals": {
        "raw_file": (
            RAW_DATA_DIR
            / "fraud_signals.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "fraud_signals.schema.json"
        ),
        "primary_key": "transaction_id",
        "timestamp_field": "score_timestamp",
    },
    "transactions": {
        "raw_file": (
            RAW_DATA_DIR
            / "transactions.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "transactions.schema.json"
        ),
        "primary_key": "transaction_id",
        "timestamp_field": (
            "transaction_timestamp"
        ),
    },
}


def utc_now() -> str:
    return (
        datetime.now(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def generate_run_id() -> str:
    timestamp = datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    short_uuid = uuid.uuid4().hex[:8]

    return f"{timestamp}_{short_uuid}"


def validate_run_id(run_id: str) -> str:
    """
    Validate the pipeline run ID format.

    Expected example:
    20260729T213000Z_a1b2c3d4
    """

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match "
            "YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )

    return run_id


def project_relative(
    path: Path | None,
) -> str | None:
    """
    Return a repository-relative path for
    reports and logs.
    """

    if path is None:
        return None

    try:
        return str(
            path.relative_to(PROJECT_ROOT)
        )
    except ValueError:
        return str(path)


class MalformedJsonLinesError(Exception):
    """
    Raised when a JSON Lines file contains
    one or more malformed lines.
    """

    def __init__(
        self,
        file_path: Path,
        errors: list[dict[str, Any]],
    ) -> None:
        self.file_path = Path(file_path)
        self.errors = errors

        super().__init__(
            f"{self.file_path} contains "
            f"{len(errors)} malformed JSON line(s)."
        )


def load_json_lines(
    file_path: Path,
) -> list[tuple[int, dict[str, Any]]]:
    records: list[
        tuple[int, dict[str, Any]]
    ] = []

    parse_errors: list[
        dict[str, Any]
    ] = []

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line_number, line in enumerate(
            file,
            start=1,
        ):
            stripped_line = line.strip()

            if not stripped_line:
                continue

            try:
                parsed_record = json.loads(
                    stripped_line
                )

                if not isinstance(
                    parsed_record,
                    dict,
                ):
                    parse_errors.append(
                        {
                            "line_number": (
                                line_number
                            ),
                            "message": (
                                "JSON line must "
                                "contain an object."
                            ),
                            "raw_line": (
                                stripped_line
                            ),
                        }
                    )
                    continue

                records.append(
                    (
                        line_number,
                        parsed_record,
                    )
                )

            except json.JSONDecodeError as error:
                parse_errors.append(
                    {
                        "line_number": line_number,
                        "message": error.msg,
                        "raw_line": stripped_line,
                    }
                )

    if parse_errors:
        raise MalformedJsonLinesError(
            file_path,
            parse_errors,
        )

    return records


def load_schema(
    schema_path: Path,
) -> dict[str, Any]:
    with schema_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def get_record_id(
    record: dict[str, Any],
    primary_key: str,
) -> Any:
    return record.get(
        primary_key,
        "UNKNOWN",
    )


def normalize_jsonschema_error(
    error: Any,
) -> dict[str, Any]:
    if error.path:
        field = ".".join(
            str(part)
            for part in error.path
        )
    else:
        field = "record"

    return {
        "field": field,
        "rule": error.validator,
        "message": error.message,
        "severity": "hard_fail",
    }


def validate_schema(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    schema: dict[str, Any],
    primary_key: str,
) -> tuple[
    list[dict[str, Any]],
    set[int],
]:
    validator = Draft202012Validator(
        schema
    )

    failures: list[
        dict[str, Any]
    ] = []

    invalid_record_indexes: set[
        int
    ] = set()

    for index, (
        line_number,
        record,
    ) in enumerate(records):
        errors = sorted(
            validator.iter_errors(record),
            key=lambda error: list(
                error.path
            ),
        )

        for error in errors:
            error_detail = (
                normalize_jsonschema_error(
                    error
                )
            )

            failures.append(
                {
                    "dataset": dataset_name,
                    "line_number": (
                        line_number
                    ),
                    "record_id": (
                        get_record_id(
                            record,
                            primary_key,
                        )
                    ),
                    **error_detail,
                }
            )

            invalid_record_indexes.add(
                index
            )

    return (
        failures,
        invalid_record_indexes,
    )


def load_reference_keys(
    parent_file: Path,
    parent_key: str,
) -> set[Any]:
    parent_records = load_json_lines(
        parent_file
    )

    return {
        record.get(parent_key)
        for _, record in parent_records
        if record.get(parent_key) is not None
    }


def validate_referential_integrity(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    primary_key: str,
) -> tuple[
    list[dict[str, Any]],
    set[int],
]:
    relationship_rules = (
        REFERENCE_RULES.get(
            dataset_name,
            [],
        )
    )

    failures: list[
        dict[str, Any]
    ] = []

    invalid_record_indexes: set[
        int
    ] = set()

    for rule in relationship_rules:
        parent_keys = load_reference_keys(
            parent_file=rule["parent_file"],
            parent_key=rule["parent_key"],
        )

        child_field = rule["child_field"]
        parent_dataset = (
            rule["parent_dataset"]
        )
        parent_key = rule["parent_key"]

        for index, (
            line_number,
            record,
        ) in enumerate(records):
            child_value = record.get(
                child_field
            )

            if child_value is None:
                continue

            if child_value not in parent_keys:
                failures.append(
                    {
                        "dataset": dataset_name,
                        "line_number": (
                            line_number
                        ),
                        "record_id": (
                            get_record_id(
                                record,
                                primary_key,
                            )
                        ),
                        "field": child_field,
                        "rule": (
                            "referential_integrity"
                        ),
                        "severity": (
                            "quarantine_continue"
                        ),
                        "message": (
                            f"{dataset_name}."
                            f"{child_field} value "
                            f"{child_value} does "
                            f"not exist in "
                            f"{parent_dataset}."
                            f"{parent_key}"
                        ),
                    }
                )

                invalid_record_indexes.add(
                    index
                )

    return (
        failures,
        invalid_record_indexes,
    )


def validate_duplicate_primary_keys(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    primary_key: str,
) -> tuple[
    list[dict[str, Any]],
    set[int],
]:
    key_counts = Counter(
        record.get(primary_key)
        for _, record in records
    )

    duplicate_keys = {
        key
        for key, count
        in key_counts.items()
        if key is not None
        and count > 1
    }

    failures: list[
        dict[str, Any]
    ] = []

    invalid_record_indexes: set[
        int
    ] = set()

    for index, (
        line_number,
        record,
    ) in enumerate(records):
        record_id = record.get(
            primary_key
        )

        if record_id in duplicate_keys:
            failures.append(
                {
                    "dataset": dataset_name,
                    "line_number": (
                        line_number
                    ),
                    "record_id": record_id,
                    "field": primary_key,
                    "rule": "unique",
                    "severity": "hard_fail",
                    "message": (
                        "Duplicate primary key "
                        f"found: {record_id}"
                    ),
                }
            )

            invalid_record_indexes.add(
                index
            )

    return (
        failures,
        invalid_record_indexes,
    )


def validate_late_arriving_transactions(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    primary_key: str,
    timestamp_field: str,
) -> list[dict[str, Any]]:
    warnings: list[
        dict[str, Any]
    ] = []

    parsed_dates: list[
        datetime
    ] = []

    for _, record in records:
        raw_timestamp = record.get(
            timestamp_field
        )

        if isinstance(
            raw_timestamp,
            str,
        ):
            try:
                parsed_dates.append(
                    datetime.strptime(
                        raw_timestamp,
                        "%Y-%m-%d %H:%M:%S",
                    )
                )
            except ValueError:
                pass

    if not parsed_dates:
        return warnings

    max_timestamp = max(
        parsed_dates
    )

    for (
        line_number,
        record,
    ) in records:
        raw_timestamp = record.get(
            timestamp_field
        )

        if not isinstance(
            raw_timestamp,
            str,
        ):
            continue

        try:
            record_timestamp = (
                datetime.strptime(
                    raw_timestamp,
                    "%Y-%m-%d %H:%M:%S",
                )
            )
        except ValueError:
            continue

        days_behind_latest = (
            max_timestamp
            - record_timestamp
        ).days

        if days_behind_latest > 365:
            warnings.append(
                {
                    "dataset": dataset_name,
                    "line_number": (
                        line_number
                    ),
                    "record_id": (
                        get_record_id(
                            record,
                            primary_key,
                        )
                    ),
                    "field": (
                        timestamp_field
                    ),
                    "rule": (
                        "late_arriving_event"
                    ),
                    "severity": (
                        "warn_continue"
                    ),
                    "message": (
                        "Transaction timestamp "
                        f"is {days_behind_latest} "
                        "days older than latest "
                        "record in batch."
                    ),
                }
            )

    return warnings


def write_quarantine_file(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    invalid_record_indexes: set[int],
    quarantine_run_dir: Path,
) -> Path:
    quarantine_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_path = (
        quarantine_run_dir
        / (
            f"{dataset_name}"
            "_invalid_records.json"
        )
    )

    with quarantine_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        for index in sorted(
            invalid_record_indexes
        ):
            _, record = records[index]

            file.write(
                json.dumps(record)
                + "\n"
            )

    return quarantine_path


def write_validated_file(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    invalid_record_indexes: set[int],
    validated_run_dir: Path,
) -> Path:
    validated_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    validated_path = (
        validated_run_dir
        / f"{dataset_name}.json"
    )

    with validated_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        for index, (
            _,
            record,
        ) in enumerate(records):
            if (
                index
                not in invalid_record_indexes
            ):
                file.write(
                    json.dumps(record)
                    + "\n"
                )

    return validated_path


def write_validation_audit_log(
    run_id: str,
    dataset_name: str,
    report: dict[str, Any],
    report_path: Path,
    reports_run_dir: Path,
) -> Path:
    reports_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    audit_log_path = (
        reports_run_dir
        / "validation_audit_log.jsonl"
    )

    audit_record = {
        "run_id": run_id,
        "validation_run_at_utc": utc_now(),
        "dataset": dataset_name,
        "contract_version": (
            report["contract_version"]
        ),
        "batch_status": (
            report["batch_status"]
        ),
        "pipeline_action": (
            report["pipeline_action"]
        ),
        "total_records": (
            report["total_records"]
        ),
        "valid_records": (
            report["valid_records"]
        ),
        "invalid_records": (
            report["invalid_records"]
        ),
        "warning_count": (
            report["warning_count"]
        ),
        "error_count_by_severity": (
            report[
                "error_count_by_severity"
            ]
        ),
        "report_file": (
            project_relative(
                report_path
            )
        ),
        "quarantine_file": (
            report["quarantine_file"]
        ),
        "validated_file": (
            report["validated_file"]
        ),
    }

    with audit_log_path.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(audit_record)
            + "\n"
        )

    return audit_log_path


def write_validation_report(
    dataset_name: str,
    report: dict[str, Any],
    reports_run_dir: Path,
) -> Path:
    reports_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = (
        reports_run_dir
        / (
            f"{dataset_name}"
            "_validation_report.json"
        )
    )

    with report_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
        )

    return report_path


def validate_dataset(
    run_id: str,
    dataset_name: str,
    config: dict[str, Any],
    validated_run_dir: Path,
    quarantine_run_dir: Path,
    reports_run_dir: Path,
    input_file_override: str | None = None,
) -> str:
    raw_file = (
        Path(input_file_override)
        if input_file_override
        else config["raw_file"]
    )

    try:
        records = load_json_lines(
            raw_file
        )

    except MalformedJsonLinesError as error:
        batch_status = "FAILED"
        pipeline_action = (
            "BLOCK_S3_UPLOAD"
        )

        failed_rules = [
            {
                "rule_name": (
                    "valid_json_lines"
                ),
                "severity": "hard_fail",
                "record_id": "UNKNOWN",
                "line_number": (
                    parse_error[
                        "line_number"
                    ]
                ),
                "field": None,
                "message": (
                    parse_error["message"]
                ),
                "invalid_value": (
                    parse_error["raw_line"]
                ),
            }
            for parse_error in error.errors
        ]

        stale_validated_path = (
            validated_run_dir
            / f"{dataset_name}.json"
        )

        if stale_validated_path.exists():
            stale_validated_path.unlink()

        severity_counts = Counter(
            failure["severity"]
            for failure in failed_rules
        )

        report = {
            "run_id": run_id,
            "validation_run_at_utc": (
                utc_now()
            ),
            "dataset": dataset_name,
            "contract_version": (
                CONTRACT_VERSION
            ),
            "batch_status": batch_status,
            "pipeline_action": (
                pipeline_action
            ),
            "total_records": len(
                error.errors
            ),
            "valid_records": 0,
            "invalid_records": len(
                error.errors
            ),
            "warning_count": 0,
            "error_count_by_severity": (
                dict(severity_counts)
            ),
            "quarantine_file": None,
            "validated_file": None,
            "failed_rules": failed_rules,
        }

        report_path = (
            write_validation_report(
                dataset_name=dataset_name,
                report=report,
                reports_run_dir=(
                    reports_run_dir
                ),
            )
        )

        audit_log_path = (
            write_validation_audit_log(
                run_id=run_id,
                dataset_name=dataset_name,
                report=report,
                report_path=report_path,
                reports_run_dir=(
                    reports_run_dir
                ),
            )
        )

        print(
            f"Dataset: {dataset_name}"
        )
        print(
            f"Run ID: {run_id}"
        )
        print(
            f"Status: {batch_status}"
        )
        print(
            "Pipeline Action: "
            f"{pipeline_action}"
        )
        print(
            "Total Records: "
            f"{len(error.errors)}"
        )
        print(
            "Invalid Records: "
            f"{len(error.errors)}"
        )
        print("Warnings: 0")
        print(
            "Malformed JSON Lines: "
            f"{len(error.errors)}"
        )
        print(
            f"Report: {report_path}"
        )
        print(
            f"Audit Log: "
            f"{audit_log_path}"
        )
        print()

        return batch_status

    schema = load_schema(
        config["schema_file"]
    )

    primary_key = config[
        "primary_key"
    ]

    (
        schema_failures,
        schema_invalid_indexes,
    ) = validate_schema(
        dataset_name=dataset_name,
        records=records,
        schema=schema,
        primary_key=primary_key,
    )

    (
        duplicate_failures,
        duplicate_invalid_indexes,
    ) = validate_duplicate_primary_keys(
        dataset_name=dataset_name,
        records=records,
        primary_key=primary_key,
    )

    (
        relationship_failures,
        relationship_invalid_indexes,
    ) = validate_referential_integrity(
        dataset_name=dataset_name,
        records=records,
        primary_key=primary_key,
    )

    warnings = (
        validate_late_arriving_transactions(
            dataset_name=dataset_name,
            records=records,
            primary_key=primary_key,
            timestamp_field=(
                config["timestamp_field"]
            ),
        )
    )

    failed_rules = (
        schema_failures
        + duplicate_failures
        + relationship_failures
        + warnings
    )

    hard_failures = [
        failure
        for failure in failed_rules
        if (
            failure["severity"]
            == "hard_fail"
        )
    ]

    quarantine_continue_failures = [
        failure
        for failure in failed_rules
        if (
            failure["severity"]
            == "quarantine_continue"
        )
    ]

    warn_continue_failures = [
        failure
        for failure in failed_rules
        if (
            failure["severity"]
            == "warn_continue"
        )
    ]

    invalid_record_indexes = (
        schema_invalid_indexes
        | duplicate_invalid_indexes
        | relationship_invalid_indexes
    )

    if hard_failures:
        batch_status = "FAILED"
        pipeline_action = (
            "BLOCK_S3_UPLOAD"
        )
    elif quarantine_continue_failures:
        batch_status = (
            "PASSED_WITH_QUARANTINE"
        )
        pipeline_action = (
            "UPLOAD_VALID_RECORDS_ONLY"
        )
    elif warn_continue_failures:
        batch_status = (
            "PASSED_WITH_WARNINGS"
        )
        pipeline_action = (
            "UPLOAD_ALL_RECORDS"
        )
    else:
        batch_status = "PASSED"
        pipeline_action = (
            "UPLOAD_ALL_RECORDS"
        )

    quarantine_path: Path | None = None
    validated_path: Path | None = None

    if invalid_record_indexes:
        quarantine_path = (
            write_quarantine_file(
                dataset_name=dataset_name,
                records=records,
                invalid_record_indexes=(
                    invalid_record_indexes
                ),
                quarantine_run_dir=(
                    quarantine_run_dir
                ),
            )
        )

    if batch_status == "FAILED":
        stale_validated_path = (
            validated_run_dir
            / f"{dataset_name}.json"
        )

        if stale_validated_path.exists():
            stale_validated_path.unlink()
    else:
        validated_path = (
            write_validated_file(
                dataset_name=dataset_name,
                records=records,
                invalid_record_indexes=(
                    invalid_record_indexes
                ),
                validated_run_dir=(
                    validated_run_dir
                ),
            )
        )

    severity_counts = Counter(
        failure["severity"]
        for failure in failed_rules
    )

    report = {
        "run_id": run_id,
        "validation_run_at_utc": utc_now(),
        "dataset": dataset_name,
        "contract_version": (
            CONTRACT_VERSION
        ),
        "batch_status": batch_status,
        "pipeline_action": (
            pipeline_action
        ),
        "total_records": len(records),
        "valid_records": (
            len(records)
            - len(invalid_record_indexes)
        ),
        "invalid_records": len(
            invalid_record_indexes
        ),
        "warning_count": len(
            warn_continue_failures
        ),
        "error_count_by_severity": (
            dict(severity_counts)
        ),
        "quarantine_file": (
            project_relative(
                quarantine_path
            )
        ),
        "validated_file": (
            project_relative(
                validated_path
            )
        ),
        "failed_rules": failed_rules,
    }

    report_path = (
        write_validation_report(
            dataset_name=dataset_name,
            report=report,
            reports_run_dir=(
                reports_run_dir
            ),
        )
    )

    audit_log_path = (
        write_validation_audit_log(
            run_id=run_id,
            dataset_name=dataset_name,
            report=report,
            report_path=report_path,
            reports_run_dir=(
                reports_run_dir
            ),
        )
    )

    print(
        f"Dataset: {dataset_name}"
    )
    print(
        f"Run ID: {run_id}"
    )
    print(
        f"Status: {batch_status}"
    )
    print(
        "Pipeline Action: "
        f"{pipeline_action}"
    )
    print(
        f"Total Records: {len(records)}"
    )
    print(
        "Invalid Records: "
        f"{len(invalid_record_indexes)}"
    )
    print(
        "Warnings: "
        f"{len(warn_continue_failures)}"
    )
    print(
        f"Report: {report_path}"
    )
    print(
        f"Audit Log: {audit_log_path}"
    )

    if quarantine_path:
        print(
            "Quarantine File: "
            f"{quarantine_path}"
        )

    if validated_path:
        print(
            "Validated File: "
            f"{validated_path}"
        )

    print()

    return batch_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate raw data files against "
            "versioned data contracts."
        )
    )

    parser.add_argument(
        "--dataset",
        choices=DATASETS.keys(),
        help="Dataset to validate.",
    )

    parser.add_argument(
        "--input-file",
        help=(
            "Optional input file override "
            "for testing fixtures."
        ),
    )

    parser.add_argument(
        "--run-id",
        type=validate_run_id,
        help=(
            "Pipeline run ID. When omitted, "
            "a new standalone run ID is generated."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if (
        args.input_file
        and not args.dataset
    ):
        raise SystemExit(
            "--dataset is required when "
            "using --input-file."
        )

    run_id = (
        args.run_id
        or generate_run_id()
    )

    validated_run_dir = (
        VALIDATED_DATA_ROOT
        / run_id
    )

    reports_run_dir = (
        REPORTS_ROOT
        / run_id
    )

    quarantine_run_dir = (
        QUARANTINE_ROOT
        / run_id
        / "invalid_records"
    )

    # A repeated run ID should produce a clean,
    # deterministic set of output files.
    for output_dir in (
        validated_run_dir,
        reports_run_dir,
        quarantine_run_dir,
    ):
        if output_dir.exists():
            shutil.rmtree(
                output_dir
            )

    validated_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    reports_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "Starting contract validation"
    )
    print(
        f"Pipeline Run ID: {run_id}"
    )
    print(
        "Validated output: "
        f"{validated_run_dir}"
    )
    print(
        "Validation reports: "
        f"{reports_run_dir}"
    )
    print()

    overall_status = "PASSED"

    if args.dataset:
        datasets_to_run = {
            args.dataset: (
                DATASETS[args.dataset]
            )
        }
    else:
        datasets_to_run = DATASETS

    for (
        dataset_name,
        config,
    ) in datasets_to_run.items():
        dataset_input_file = (
            args.input_file
            if args.dataset == dataset_name
            else None
        )

        dataset_status = validate_dataset(
            run_id=run_id,
            dataset_name=dataset_name,
            config=config,
            validated_run_dir=(
                validated_run_dir
            ),
            quarantine_run_dir=(
                quarantine_run_dir
            ),
            reports_run_dir=(
                reports_run_dir
            ),
            input_file_override=(
                dataset_input_file
            ),
        )

        if dataset_status == "FAILED":
            overall_status = "FAILED"

    print(
        "Contract validation completed."
    )
    print(
        f"Pipeline Run ID: {run_id}"
    )
    print(
        f"Overall Status: {overall_status}"
    )

    if overall_status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()