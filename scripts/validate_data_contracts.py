import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
CONTRACTS_DIR = PROJECT_ROOT / "contracts" / "v1"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine" / "invalid_records"
REPORTS_DIR = PROJECT_ROOT / "data" / "validation_reports"

CONTRACT_VERSION = "v1"


DATASETS = {
    "customers": {
        "raw_file": RAW_DATA_DIR / "customers.json",
        "schema_file": CONTRACTS_DIR / "customers.schema.json",
        "primary_key": "customer_id",
        "timestamp_field": "created_at",
    },
    "fraud_signals": {
        "raw_file": RAW_DATA_DIR / "fraud_signals.json",
        "schema_file": CONTRACTS_DIR / "fraud_signals.schema.json",
        "primary_key": "transaction_id",
        "timestamp_field": "score_timestamp",
    },
    "transactions": {
        "raw_file": RAW_DATA_DIR / "transactions.json",
        "schema_file": CONTRACTS_DIR / "transactions.schema.json",
        "primary_key": "transaction_id",
        "timestamp_field": "transaction_timestamp",
    },
}


def load_json_lines(file_path):
    records = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if line.strip():
                records.append((line_number, json.loads(line)))

    return records


def load_schema(schema_path):
    with open(schema_path, "r", encoding="utf-8") as file:
        return json.load(file)


def get_record_id(record, primary_key):
    return record.get(primary_key, "UNKNOWN")


def normalize_jsonschema_error(error):
    if error.path:
        field = ".".join(str(part) for part in error.path)
    else:
        field = "record"

    return {
        "field": field,
        "rule": error.validator,
        "message": error.message,
        "severity": "hard_fail",
    }


def validate_schema(dataset_name, records, schema, primary_key):
    validator = Draft202012Validator(schema)
    failures = []
    invalid_record_indexes = set()

    for index, (line_number, record) in enumerate(records):
        errors = sorted(validator.iter_errors(record), key=lambda error: list(error.path))

        for error in errors:
            error_detail = normalize_jsonschema_error(error)

            failures.append(
                {
                    "dataset": dataset_name,
                    "line_number": line_number,
                    "record_id": get_record_id(record, primary_key),
                    **error_detail,
                }
            )

            invalid_record_indexes.add(index)

    return failures, invalid_record_indexes


def validate_duplicate_primary_keys(dataset_name, records, primary_key):
    key_counts = Counter(record.get(primary_key) for _, record in records)
    duplicate_keys = {key for key, count in key_counts.items() if key is not None and count > 1}

    failures = []
    invalid_record_indexes = set()

    for index, (line_number, record) in enumerate(records):
        record_id = record.get(primary_key)

        if record_id in duplicate_keys:
            failures.append(
                {
                    "dataset": dataset_name,
                    "line_number": line_number,
                    "record_id": record_id,
                    "field": primary_key,
                    "rule": "unique",
                    "severity": "hard_fail",
                    "message": f"Duplicate primary key found: {record_id}",
                }
            )

            invalid_record_indexes.add(index)

    return failures, invalid_record_indexes


def validate_late_arriving_transactions(dataset_name, records, primary_key, timestamp_field):
    warnings = []

    parsed_dates = []

    for _, record in records:
        raw_timestamp = record.get(timestamp_field)

        if isinstance(raw_timestamp, str):
            try:
                parsed_dates.append(datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                pass

    if not parsed_dates:
        return warnings

    max_timestamp = max(parsed_dates)

    for line_number, record in records:
        raw_timestamp = record.get(timestamp_field)

        if not isinstance(raw_timestamp, str):
            continue

        try:
            record_timestamp = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        days_behind_latest = (max_timestamp - record_timestamp).days

        if days_behind_latest > 365:
            warnings.append(
                {
                    "dataset": dataset_name,
                    "line_number": line_number,
                    "record_id": get_record_id(record, primary_key),
                    "field": timestamp_field,
                    "rule": "late_arriving_event",
                    "severity": "warn_continue",
                    "message": f"Transaction timestamp is {days_behind_latest} days older than latest record in batch.",
                }
            )

    return warnings


def write_quarantine_file(dataset_name, records, invalid_record_indexes):
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    quarantine_path = QUARANTINE_DIR / f"{dataset_name}_invalid_records.json"

    with open(quarantine_path, "w", encoding="utf-8") as file:
        for index in sorted(invalid_record_indexes):
            _, record = records[index]
            file.write(json.dumps(record) + "\n")

    return quarantine_path


def write_validation_report(dataset_name, report):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    report_path = REPORTS_DIR / f"{dataset_name}_validation_report.json"

    with open(report_path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    return report_path


def validate_dataset(dataset_name, config, input_file_override=None):
    raw_file = Path(input_file_override) if input_file_override else config["raw_file"]
    records = load_json_lines(raw_file)
    schema = load_schema(config["schema_file"])

    primary_key = config["primary_key"]

    schema_failures, schema_invalid_indexes = validate_schema(
        dataset_name=dataset_name,
        records=records,
        schema=schema,
        primary_key=primary_key,
    )

    duplicate_failures, duplicate_invalid_indexes = validate_duplicate_primary_keys(
        dataset_name=dataset_name,
        records=records,
        primary_key=primary_key,
    )

    warnings = validate_late_arriving_transactions(
        dataset_name=dataset_name,
        records=records,
        primary_key=primary_key,
        timestamp_field=config["timestamp_field"],
    )

    failed_rules = schema_failures + duplicate_failures + warnings

    hard_failures = [failure for failure in failed_rules if failure["severity"] == "hard_fail"]
    quarantine_continue_failures = [
        failure for failure in failed_rules if failure["severity"] == "quarantine_continue"
    ]
    warn_continue_failures = [
        failure for failure in failed_rules if failure["severity"] == "warn_continue"
    ]

    invalid_record_indexes = schema_invalid_indexes | duplicate_invalid_indexes

    if hard_failures:
        batch_status = "FAILED"
        pipeline_action = "BLOCK_S3_UPLOAD"
    elif quarantine_continue_failures:
        batch_status = "PASSED_WITH_QUARANTINE"
        pipeline_action = "UPLOAD_VALID_RECORDS_ONLY"
    elif warn_continue_failures:
        batch_status = "PASSED_WITH_WARNINGS"
        pipeline_action = "UPLOAD_ALL_RECORDS"
    else:
        batch_status = "PASSED"
        pipeline_action = "UPLOAD_ALL_RECORDS"

    quarantine_path = None

    if invalid_record_indexes:
        quarantine_path = write_quarantine_file(dataset_name, records, invalid_record_indexes)

    severity_counts = Counter(failure["severity"] for failure in failed_rules)

    report = {
        "dataset": dataset_name,
        "contract_version": CONTRACT_VERSION,
        "batch_status": batch_status,
        "pipeline_action": pipeline_action,
        "total_records": len(records),
        "valid_records": len(records) - len(invalid_record_indexes),
        "invalid_records": len(invalid_record_indexes),
        "warning_count": len(warn_continue_failures),
        "error_count_by_severity": dict(severity_counts),
        "quarantine_file": str(quarantine_path) if quarantine_path else None,
        "failed_rules": failed_rules,
    }

    report_path = write_validation_report(dataset_name, report)

    print(f"Dataset: {dataset_name}")
    print(f"Status: {batch_status}")
    print(f"Pipeline Action: {pipeline_action}")
    print(f"Total Records: {len(records)}")
    print(f"Invalid Records: {len(invalid_record_indexes)}")
    print(f"Warnings: {len(warn_continue_failures)}")
    print(f"Report: {report_path}")

    if quarantine_path:
        print(f"Quarantine File: {quarantine_path}")

    return batch_status


def main():
    parser = argparse.ArgumentParser(description="Validate raw data files against versioned data contracts.")
    parser.add_argument("--dataset", choices=DATASETS.keys(), help="Dataset to validate.")
    parser.add_argument("--input-file", help="Optional input file override for testing fixtures.")
    args = parser.parse_args()

    if args.input_file and not args.dataset:
        raise SystemExit("--dataset is required when using --input-file.")

    overall_status = "PASSED"

    datasets_to_run = {args.dataset: DATASETS[args.dataset]} if args.dataset else DATASETS

    for dataset_name, config in datasets_to_run.items():
        dataset_input_file = args.input_file if args.dataset == dataset_name else None

        dataset_status = validate_dataset(
            dataset_name=dataset_name,
            config=config,
            input_file_override=dataset_input_file,
        )

        if dataset_status == "FAILED":
            overall_status = "FAILED"

    if overall_status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
