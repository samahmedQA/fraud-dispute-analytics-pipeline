from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_ROOT = PROJECT_ROOT / "data" / "raw"
VALIDATED_DATA_ROOT = PROJECT_ROOT / "data" / "validated"
CONTRACTS_DIR = PROJECT_ROOT / "contracts" / "v1"
QUARANTINE_ROOT = PROJECT_ROOT / "data" / "quarantine"
REPORTS_ROOT = PROJECT_ROOT / "data" / "validation_reports"

CONTRACT_VERSION = "v1"

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
)


REFERENCE_RULES = {
    "transactions": [
        {
            "child_fields": (
                "customer_id",
            ),
            "parent_dataset": "customers",
            "parent_fields": (
                "customer_id",
            ),
        },
        {
            "child_fields": (
                "customer_id",
                "account_id",
            ),
            "parent_dataset": "customers",
            "parent_fields": (
                "customer_id",
                "account_id",
            ),
        },
    ],
    "fraud_signals": [
        {
            "child_fields": (
                "transaction_id",
            ),
            "parent_dataset": "transactions",
            "parent_fields": (
                "transaction_id",
            ),
        },
    ],
    "disputes": [
        {
            "child_fields": (
                "transaction_id",
            ),
            "parent_dataset": "transactions",
            "parent_fields": (
                "transaction_id",
            ),
        },
    ],
    "chargeback_outcomes": [
        {
            "child_fields": (
                "dispute_id",
            ),
            "parent_dataset": "disputes",
            "parent_fields": (
                "dispute_id",
            ),
        },
    ],
}


# Dictionary order is the validation order.
# Every parent dataset must be validated before its children.
DATASETS = {
    "customers": {
        "raw_file_name": "customers.json",
        "schema_file": (
            CONTRACTS_DIR
            / "customers.schema.json"
        ),
        "primary_key": "customer_id",
        "timestamp_field": "created_at",
    },
    "transactions": {
        "raw_file_name": "transactions.json",
        "schema_file": (
            CONTRACTS_DIR
            / "transactions.schema.json"
        ),
        "primary_key": "transaction_id",
        "timestamp_field": (
            "transaction_timestamp"
        ),
    },
    "fraud_signals": {
        "raw_file_name": "fraud_signals.json",
        "schema_file": (
            CONTRACTS_DIR
            / "fraud_signals.schema.json"
        ),
        "primary_key": "transaction_id",
        "timestamp_field": "score_timestamp",
    },
    "disputes": {
        "raw_file_name": "disputes.json",
        "schema_file": (
            CONTRACTS_DIR
            / "disputes.schema.json"
        ),
        "primary_key": "dispute_id",
        "timestamp_field": "opened_date",
    },
    "chargeback_outcomes": {
        "raw_file_name": (
            "chargeback_outcomes.json"
        ),
        "schema_file": (
            CONTRACTS_DIR
            / "chargeback_outcomes.schema.json"
        ),
        "primary_key": "chargeback_id",
        "timestamp_field": "resolved_date",
    },
}


ReferenceKey = tuple[Any, ...]
ReferenceRegistry = dict[
    tuple[str, tuple[str, ...]],
    set[ReferenceKey],
]


def utc_now() -> str:
    return (
        datetime.now(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def calculate_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_and_verify_raw_manifest(
    run_id: str,
    raw_run_dir: Path,
) -> dict[str, Any]:
    if not raw_run_dir.is_dir():
        raise SystemExit(
            "Raw-data directory does not exist for run ID "
            f"{run_id}: {raw_run_dir}"
        )

    manifest_path = raw_run_dir / "raw_manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(
            "Raw-data manifest does not exist for run ID "
            f"{run_id}: {manifest_path}"
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise SystemExit(
            f"Raw-data manifest is invalid JSON: {manifest_path}: {error}"
        ) from error

    if manifest.get("run_id") != run_id:
        raise SystemExit(
            "Raw manifest run ID does not match requested run ID "
            f"{run_id}."
        )

    files = manifest.get("files")
    if not isinstance(files, dict):
        raise SystemExit("Raw-data manifest has no valid files object.")

    for dataset_name, config in DATASETS.items():
        metadata = files.get(dataset_name)
        if not isinstance(metadata, dict):
            raise SystemExit(
                f"Raw-data manifest is missing dataset {dataset_name}."
            )

        file_name = config["raw_file_name"]
        if metadata.get("file_name") != file_name:
            raise SystemExit(
                f"Raw-data manifest file mismatch for {dataset_name}."
            )

        file_path = raw_run_dir / file_name
        if not file_path.is_file():
            raise SystemExit(f"Raw-data file is missing: {file_path}")

        if file_path.stat().st_size != metadata.get("file_size_bytes"):
            raise SystemExit(
                f"Raw-data file size does not match manifest: {file_path}"
            )

        if calculate_sha256(file_path) != metadata.get("sha256"):
            raise SystemExit(
                f"Raw-data file hash does not match manifest: {file_path}"
            )

    return manifest


def generate_run_id() -> str:
    timestamp = datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    short_uuid = uuid.uuid4().hex[:8]

    return f"{timestamp}_{short_uuid}"


SEMANTIC_FORMAT_CHECKER = FormatChecker()


def matches_datetime_format(
    value: object,
    expected_format: str,
) -> bool:
    """
    Validate both the shape and calendar meaning of a date/time string.

    Returning True for non-string values allows JSON Schema's type
    validation to report the appropriate type failure separately.
    """
    if not isinstance(value, str):
        return True

    try:
        parsed_value = datetime.strptime(
            value,
            expected_format,
        )
    except ValueError:
        return False

    return (
        parsed_value.strftime(expected_format)
        == value
    )


@SEMANTIC_FORMAT_CHECKER.checks(
    "pipeline-date"
)
def is_pipeline_date(
    value: object,
) -> bool:
    """Validate YYYY-MM-DD as a real calendar date."""
    return matches_datetime_format(
        value,
        "%Y-%m-%d",
    )


@SEMANTIC_FORMAT_CHECKER.checks(
    "pipeline-date-time"
)
def is_pipeline_date_time(
    value: object,
) -> bool:
    """
    Validate YYYY-MM-DD HH:MM:SS as a real timestamp.
    """
    return matches_datetime_format(
        value,
        "%Y-%m-%d %H:%M:%S",
    )


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
        schema,
        format_checker=SEMANTIC_FORMAT_CHECKER,
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


def make_reference_key(
    record: dict[str, Any],
    fields: tuple[str, ...],
) -> ReferenceKey | None:
    values = tuple(
        record.get(field)
        for field in fields
    )

    if any(
        value is None
        for value in values
    ):
        return None

    return values


def required_parent_field_sets(
    dataset_name: str,
) -> set[tuple[str, ...]]:
    return {
        tuple(rule["parent_fields"])
        for rules in REFERENCE_RULES.values()
        for rule in rules
        if (
            rule["parent_dataset"]
            == dataset_name
        )
    }


def register_valid_parent_keys(
    dataset_name: str,
    valid_records: list[
        dict[str, Any]
    ],
    registry: ReferenceRegistry,
) -> None:
    for parent_fields in (
        required_parent_field_sets(
            dataset_name
        )
    ):
        keys: set[ReferenceKey] = set()

        for record in valid_records:
            key = make_reference_key(
                record,
                parent_fields,
            )

            if key is not None:
                keys.add(key)

        registry[
            (
                dataset_name,
                parent_fields,
            )
        ] = keys


def select_valid_records(
    records: list[
        tuple[int, dict[str, Any]]
    ],
    invalid_record_indexes: set[int],
) -> list[dict[str, Any]]:
    return [
        record
        for index, (_, record)
        in enumerate(records)
        if index not in invalid_record_indexes
    ]


def resolve_dataset_dependencies(
    dataset_name: str,
) -> tuple[str, ...]:
    required_datasets: set[str] = set()

    def visit(
        current_dataset: str,
    ) -> None:
        for rule in REFERENCE_RULES.get(
            current_dataset,
            [],
        ):
            visit(
                rule["parent_dataset"]
            )

        required_datasets.add(
            current_dataset
        )

    visit(dataset_name)

    return tuple(
        name
        for name in DATASETS
        if name in required_datasets
    )


def validate_referential_integrity(
    dataset_name: str,
    records: list[
        tuple[int, dict[str, Any]]
    ],
    primary_key: str,
    valid_parent_keys: ReferenceRegistry,
    enforce_relationships: bool = True,
) -> tuple[
    list[dict[str, Any]],
    set[int],
]:
    if not enforce_relationships:
        return (
            [],
            set(),
        )

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
        child_fields = tuple(
            rule["child_fields"]
        )
        parent_dataset = (
            rule["parent_dataset"]
        )
        parent_fields = tuple(
            rule["parent_fields"]
        )

        registry_key = (
            parent_dataset,
            parent_fields,
        )

        if registry_key not in valid_parent_keys:
            raise RuntimeError(
                "Parent dataset has not been "
                "validated before child dataset: "
                f"{parent_dataset} "
                f"required by {dataset_name}."
            )

        parent_keys = (
            valid_parent_keys[
                registry_key
            ]
        )

        for index, (
            line_number,
            record,
        ) in enumerate(records):
            child_key = (
                make_reference_key(
                    record,
                    child_fields,
                )
            )

            # Required-field and type errors are
            # reported by JSON Schema validation.
            if child_key is None:
                continue

            if child_key in parent_keys:
                continue

            child_field_label = (
                ", ".join(child_fields)
            )

            parent_field_label = (
                ", ".join(parent_fields)
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
                    "field": (
                        child_field_label
                    ),
                    "rule": (
                        "referential_integrity"
                    ),
                    "severity": (
                        "quarantine_continue"
                    ),
                    "message": (
                        f"{dataset_name}"
                        f".({child_field_label}) "
                        f"value {child_key} does "
                        "not reference a valid "
                        f"{parent_dataset}"
                        f".({parent_field_label}) "
                        "record."
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
    raw_run_dir: Path,
    validated_run_dir: Path,
    quarantine_run_dir: Path,
    reports_run_dir: Path,
    valid_parent_keys: ReferenceRegistry,
    enforce_relationships: bool = True,
    input_file_override: str | None = None,
) -> tuple[
    str,
    list[dict[str, Any]],
]:
    raw_file = (
        Path(input_file_override)
        if input_file_override
        else raw_run_dir / config["raw_file_name"]
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

        return (
            batch_status,
            [],
        )

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
        valid_parent_keys=(
            valid_parent_keys
        ),
        enforce_relationships=(
            enforce_relationships
        ),
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

    valid_records = select_valid_records(
        records=records,
        invalid_record_indexes=(
            invalid_record_indexes
        ),
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
        "valid_records": len(
            valid_records
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

    return (
        batch_status,
        valid_records,
    )


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

    if not args.run_id and not args.input_file:
        raise SystemExit(
            "--run-id is required unless "
            "--input-file is used."
        )

    run_id = (
        args.run_id
        or generate_run_id()
    )

    raw_run_dir = (
        RAW_DATA_ROOT
        / run_id
    )

    # Fixture overrides validate one standalone file.
    # They do not require a complete run snapshot.
    requires_raw_snapshot = not args.input_file

    if requires_raw_snapshot:
        load_and_verify_raw_manifest(
            run_id=run_id,
            raw_run_dir=raw_run_dir,
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

    # A repeated run ID should produce a
    # clean, deterministic output set.
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

    enforce_relationships = (
        not args.input_file
    )

    if (
        args.input_file
        and args.dataset
        in REFERENCE_RULES
    ):
        print(
            "Relationship validation skipped: "
            "input-file fixture mode does not "
            "load parent datasets."
        )

    print()

    overall_status = "PASSED"

    if args.dataset:
        if (
            args.run_id
            and not args.input_file
        ):
            dataset_names = (
                resolve_dataset_dependencies(
                    args.dataset
                )
            )
        else:
            dataset_names = (
                args.dataset,
            )

        datasets_to_run = {
            dataset_name: (
                DATASETS[dataset_name]
            )
            for dataset_name
            in dataset_names
        }
    else:
        datasets_to_run = DATASETS

    valid_parent_keys: (
        ReferenceRegistry
    ) = {}

    for (
        dataset_name,
        config,
    ) in datasets_to_run.items():
        dataset_input_file = (
            args.input_file
            if (
                args.dataset
                == dataset_name
            )
            else None
        )

        (
            dataset_status,
            valid_records,
        ) = validate_dataset(
            run_id=run_id,
            dataset_name=dataset_name,
            config=config,
            raw_run_dir=raw_run_dir,
            validated_run_dir=(
                validated_run_dir
            ),
            quarantine_run_dir=(
                quarantine_run_dir
            ),
            reports_run_dir=(
                reports_run_dir
            ),
            valid_parent_keys=(
                valid_parent_keys
            ),
            enforce_relationships=(
                enforce_relationships
            ),
            input_file_override=(
                dataset_input_file
            ),
        )

        register_valid_parent_keys(
            dataset_name=dataset_name,
            valid_records=valid_records,
            registry=valid_parent_keys,
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