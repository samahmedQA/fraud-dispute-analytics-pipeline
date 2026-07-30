from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path, PurePosixPath
from typing import Any

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARTITIONED_ROOT = PROJECT_ROOT / "data" / "s3_partitioned"

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
)

REQUIRED_DATASETS = (
    "customers",
    "transactions",
    "fraud_signals",
    "disputes",
    "chargeback_outcomes",
)

TEMP_TABLES = {
    "customers": "TMP_RAW_CUSTOMERS",
    "transactions": "TMP_RAW_TRANSACTIONS",
    "fraud_signals": "TMP_RAW_FRAUD_SIGNALS",
    "disputes": "TMP_RAW_DISPUTES",
    "chargeback_outcomes": (
        "TMP_RAW_CHARGEBACK_OUTCOMES"
    ),
}

GUARDRAIL_MARKER = (
    "SELECT "
    "'__PIPELINE_GUARDRAIL_VALIDATE_TEMP_LOAD__'"
)


def validate_run_id(run_id: str) -> str:
    """Validate the pipeline run ID format."""

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match "
            "YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )

    return run_id


def remove_sql_comments(sql_text: str) -> str:
    cleaned_lines = []

    for line in sql_text.lstrip("\ufeff").splitlines():
        stripped_line = line.strip()

        if stripped_line.startswith("--"):
            continue

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


def split_sql_statements(sql_text: str) -> list[str]:
    statements = []
    sql_without_comments = remove_sql_comments(sql_text)

    for statement in sql_without_comments.split(";"):
        cleaned_statement = statement.strip()

        if cleaned_statement:
            statements.append(cleaned_statement + ";")

    return statements


def count_json_lines(file_path: Path) -> int:
    count = 0

    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            stripped_line = line.strip()

            if not stripped_line:
                continue

            try:
                json.loads(stripped_line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    "Invalid JSON in partition file "
                    f"{file_path} at line {line_number}."
                ) from error

            count += 1

    return count


def load_partition_manifest(
    run_id: str,
) -> dict[str, Any]:
    """Load and validate one local partition manifest."""

    validate_run_id(run_id)

    run_dir = PARTITIONED_ROOT / run_id
    manifest_path = run_dir / "partition_manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(
            "Partition manifest not found: "
            f"{manifest_path}"
        )

    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as error:
        raise ValueError(
            "Partition manifest is not valid JSON: "
            f"{manifest_path}"
        ) from error

    if manifest.get("run_id") != run_id:
        raise ValueError(
            "Manifest run_id does not match the "
            f"requested run ID {run_id}."
        )

    datasets = manifest.get("datasets")

    if not isinstance(datasets, list):
        raise ValueError(
            "Manifest datasets must be a list."
        )

    dataset_names = {
        dataset.get("dataset")
        for dataset in datasets
        if isinstance(dataset, dict)
    }

    required_names = set(REQUIRED_DATASETS)

    if dataset_names != required_names:
        missing = sorted(required_names - dataset_names)
        unexpected = sorted(dataset_names - required_names)
        raise ValueError(
            "Manifest dataset set is invalid. "
            f"Missing: {missing or 'none'}. "
            f"Unexpected: {unexpected or 'none'}."
        )

    if manifest.get("dataset_count") != len(REQUIRED_DATASETS):
        raise ValueError(
            "Manifest dataset_count does not match "
            "the required dataset count."
        )

    if manifest.get("records_missing_date") != 0:
        raise ValueError(
            "Manifest contains records missing "
            "partition dates."
        )

    expected_rows: dict[str, int] = {}
    expected_files: dict[str, int] = {}
    actual_total_rows = 0
    actual_total_files = 0

    for dataset in datasets:
        dataset_name = dataset["dataset"]
        received = dataset.get("records_received")
        partitioned = dataset.get("records_partitioned")
        missing_date = dataset.get("records_missing_date")
        output_files = dataset.get("output_files")
        partition_count = dataset.get("partition_count")

        if not isinstance(received, int) or received < 1:
            raise ValueError(
                f"{dataset_name} records_received "
                "must be a positive integer."
            )

        if partitioned != received:
            raise ValueError(
                f"{dataset_name} partitioned "
                f"{partitioned} of {received} records."
            )

        if missing_date != 0:
            raise ValueError(
                f"{dataset_name} has records missing "
                "partition dates."
            )

        if not isinstance(output_files, list) or not output_files:
            raise ValueError(
                f"{dataset_name} output_files must "
                "be a non-empty list."
            )

        if partition_count != len(output_files):
            raise ValueError(
                f"{dataset_name} partition_count does "
                "not match output_files."
            )

        dataset_actual_rows = 0

        for output_file in output_files:
            normalized = str(output_file).replace("\\", "/")
            relative_path = PurePosixPath(normalized)

            if (
                relative_path.is_absolute()
                or ".." in relative_path.parts
                or relative_path.parts[:2]
                != ("raw", dataset_name)
            ):
                raise ValueError(
                    f"Invalid output path for {dataset_name}: "
                    f"{output_file}"
                )

            local_file = run_dir.joinpath(
                *relative_path.parts
            )

            if not local_file.is_file():
                raise FileNotFoundError(
                    "Manifest output file not found: "
                    f"{local_file}"
                )

            dataset_actual_rows += count_json_lines(
                local_file
            )

        if dataset_actual_rows != partitioned:
            raise ValueError(
                f"{dataset_name} local partition files "
                f"contain {dataset_actual_rows} records; "
                f"manifest expects {partitioned}."
            )

        expected_rows[dataset_name] = partitioned
        expected_files[dataset_name] = len(output_files)
        actual_total_rows += dataset_actual_rows
        actual_total_files += len(output_files)

    manifest_rows = manifest.get("records_partitioned")
    manifest_received = manifest.get("records_received")

    if manifest_received != manifest_rows:
        raise ValueError(
            "Manifest records_received does not match "
            "records_partitioned."
        )

    if actual_total_rows != manifest_rows:
        raise ValueError(
            "Manifest total row count does not match "
            "the dataset totals."
        )

    manifest["expected_rows"] = expected_rows
    manifest["expected_files"] = expected_files
    manifest["expected_total_files"] = actual_total_files
    manifest["manifest_path"] = str(manifest_path)

    return manifest


def render_sql_template(
    sql_text: str,
    run_id: str | None,
) -> str:
    """Replace validated template values in SQL."""

    if "{{RUN_ID}}" in sql_text:
        if not run_id:
            raise ValueError(
                "This SQL file requires --run-id."
            )

        validate_run_id(run_id)
        sql_text = sql_text.replace(
            "{{RUN_ID}}",
            run_id,
        )

    unresolved = re.findall(
        r"{{[A-Z0-9_]+}}",
        sql_text,
    )

    if unresolved:
        raise ValueError(
            "Unresolved SQL template values: "
            + ", ".join(sorted(set(unresolved)))
        )

    return sql_text


def get_snowflake_connection():
    load_dotenv(PROJECT_ROOT / ".env")

    required_env_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]

    missing_vars = [
        var
        for var in required_env_vars
        if not os.getenv(var)
    ]

    if missing_vars:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing_vars)
        )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )


def validate_temporary_load(
    cursor,
    run_id: str,
    manifest: dict[str, Any],
) -> None:
    """Block promotion unless every temporary table matches the manifest."""

    print("\nGuardrail checkpoint: validating temporary loads.")

    for dataset_name in REQUIRED_DATASETS:
        table_name = TEMP_TABLES[dataset_name]
        expected_rows = manifest["expected_rows"][dataset_name]
        expected_files = manifest["expected_files"][dataset_name]

        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT source_file) AS file_count,
                COUNT_IF(
                    pipeline_run_id IS NULL
                    OR pipeline_run_id <> '{run_id}'
                ) AS wrong_run_rows,
                COUNT_IF(source_file IS NULL) AS missing_source_files,
                COUNT_IF(source_row_number IS NULL) AS missing_row_numbers
            FROM {table_name};
            """
        )

        result = cursor.fetchone()

        if result is None:
            raise RuntimeError(
                f"No validation result returned for {table_name}."
            )

        (
            actual_rows,
            actual_files,
            wrong_run_rows,
            missing_source_files,
            missing_row_numbers,
        ) = result

        print(
            f"{dataset_name}: {actual_rows} rows, "
            f"{actual_files} files"
        )

        problems = []

        if actual_rows != expected_rows:
            problems.append(
                f"expected {expected_rows} rows, "
                f"found {actual_rows}"
            )

        if actual_files != expected_files:
            problems.append(
                f"expected {expected_files} files, "
                f"found {actual_files}"
            )

        if wrong_run_rows != 0:
            problems.append(
                f"found {wrong_run_rows} rows with an "
                "incorrect or missing run ID"
            )

        if missing_source_files != 0:
            problems.append(
                f"found {missing_source_files} rows without "
                "source_file metadata"
            )

        if missing_row_numbers != 0:
            problems.append(
                f"found {missing_row_numbers} rows without "
                "source_row_number metadata"
            )

        if problems:
            raise RuntimeError(
                f"Snowflake load guardrail failed for "
                f"{dataset_name}: "
                + "; ".join(problems)
            )

    print(
        "Guardrail checkpoint passed. "
        "RAW promotion may proceed."
    )


def is_guardrail_marker(statement: str) -> bool:
    normalized = statement.rstrip(";").strip()
    return normalized == GUARDRAIL_MARKER


def run_sql_file(
    sql_file_path: str,
    dry_run: bool,
    run_id: str | None = None,
) -> None:
    sql_path = PROJECT_ROOT / sql_file_path

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    original_sql = sql_path.read_text(
        encoding="utf-8-sig"
    )
    sql_text = render_sql_template(
        original_sql,
        run_id,
    )
    statements = split_sql_statements(sql_text)

    manifest = None

    if run_id:
        manifest = load_partition_manifest(run_id)
        print(f"Pipeline Run ID: {run_id}")
        print(
            "Expected load: "
            f"{manifest['records_partitioned']} records "
            f"across {manifest['expected_total_files']} files"
        )
        print(
            "Manifest: "
            f"{manifest['manifest_path']}"
        )

    if any(is_guardrail_marker(item) for item in statements):
        if not run_id or manifest is None:
            raise ValueError(
                "Guardrail SQL requires --run-id and a "
                "valid partition manifest."
            )

    print(f"SQL file: {sql_path}")
    print(f"Statements found: {len(statements)}")

    if dry_run:
        print("\nDRY RUN mode. No SQL will be executed.\n")

        for index, statement in enumerate(statements, start=1):
            first_line = statement.splitlines()[0]

            if is_guardrail_marker(statement):
                first_line = "Guardrail checkpoint"

            print(f"{index}. {first_line}")

        return

    print("\nEXECUTE mode. SQL will be executed in Snowflake.\n")

    connection = get_snowflake_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        for index, statement in enumerate(statements, start=1):
            if is_guardrail_marker(statement):
                validate_temporary_load(
                    cursor,
                    run_id,
                    manifest,
                )
                continue

            preview = statement.splitlines()[0]
            print(
                f"Executing statement {index}/{len(statements)}: "
                f"{preview}"
            )
            cursor.execute(statement)

        print("\nSQL file executed successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        if cursor is not None:
            cursor.close()

        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local SQL file against Snowflake."
    )

    parser.add_argument(
        "--sql-file",
        required=True,
        help="Path to SQL file relative to the project root.",
    )

    parser.add_argument(
        "--run-id",
        type=validate_run_id,
        help=(
            "Pipeline run ID used to select one S3 batch "
            "and validate its partition manifest."
        ),
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Actually execute SQL. Without this flag, "
            "the script runs as dry run."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_sql_file(
        sql_file_path=args.sql_file,
        dry_run=not args.execute,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
