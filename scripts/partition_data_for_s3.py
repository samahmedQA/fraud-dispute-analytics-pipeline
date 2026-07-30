from __future__ import annotations

import argparse
import json
import re
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[1]

VALIDATED_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "validated"
)

PARTITIONED_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "s3_partitioned"
)

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
)


DATASETS = {
    "customers": {
        "input_file": "customers.json",
        "date_field": "created_at",
    },
    "transactions": {
        "input_file": "transactions.json",
        "date_field": "transaction_timestamp",
    },
    "fraud_signals": {
        "input_file": "fraud_signals.json",
        "date_field": "score_timestamp",
    },
    "disputes": {
        "input_file": "disputes.json",
        "date_field": "opened_date",
    },
    "chargeback_outcomes": {
        "input_file": "chargeback_outcomes.json",
        "date_field": "resolved_date",
    },
}


def utc_now() -> str:
    """
    Return the current UTC timestamp in ISO 8601 format.
    """

    return (
        datetime.now(UTC)
        .isoformat()
        .replace("+00:00", "Z")
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Partition validated pipeline data "
            "for one specific pipeline run."
        )
    )

    parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help=(
            "Pipeline run ID whose validated "
            "files should be partitioned."
        ),
    )

    return parser.parse_args()


def parse_datetime(value: str) -> datetime:
    """
    Parse timestamp formats produced by the
    synthetic data generator.
    """

    if not value:
        raise ValueError(
            "Missing date value"
        )

    normalized_value = (
        str(value)
        .strip()
        .replace("Z", "")
    )

    try:
        return datetime.fromisoformat(
            normalized_value
        )

    except ValueError:
        return datetime.strptime(
            normalized_value[:19],
            "%Y-%m-%d %H:%M:%S",
        )


def load_json_lines(
    file_path: Path,
) -> Iterator[dict[str, Any]]:
    """
    Read newline-delimited JSON records.
    """

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
                record = json.loads(
                    stripped_line
                )

            except json.JSONDecodeError as error:
                raise ValueError(
                    "Invalid JSON in validated file "
                    f"{file_path} at line "
                    f"{line_number}: {error}"
                ) from error

            if not isinstance(record, dict):
                raise ValueError(
                    "Expected a JSON object in "
                    f"{file_path} at line "
                    f"{line_number}."
                )

            yield record


def write_partitioned_dataset(
    dataset_name: str,
    input_file: str,
    date_field: str,
    validated_run_dir: Path,
    output_raw_dir: Path,
    output_run_dir: Path,
) -> dict[str, Any]:
    """
    Partition one validated dataset by year
    and month.

    Returns metadata used in the run manifest.
    """

    input_path = (
        validated_run_dir
        / input_file
    )

    if not input_path.exists():
        raise FileNotFoundError(
            "Missing validated input file: "
            f"{input_path}. Run "
            "scripts/validate_data_contracts.py "
            "with the same --run-id before "
            "partitioning."
        )

    partitions: dict[
        tuple[int, str],
        list[dict[str, Any]],
    ] = defaultdict(list)

    records_received = 0
    records_partitioned = 0
    records_missing_date = 0

    for record in load_json_lines(
        input_path
    ):
        records_received += 1

        date_value = record.get(
            date_field
        )

        if not date_value:
            records_missing_date += 1
            continue

        try:
            parsed_date = parse_datetime(
                str(date_value)
            )

        except ValueError as error:
            raise ValueError(
                f"{dataset_name} record has an "
                f"invalid {date_field} value: "
                f"{date_value}"
            ) from error

        year = parsed_date.year
        month = f"{parsed_date.month:02d}"

        partitions[
            (year, month)
        ].append(record)

    output_files: list[str] = []

    for (
        year,
        month,
    ), records in sorted(
        partitions.items()
    ):
        partition_dir = (
            output_raw_dir
            / dataset_name
            / f"year={year}"
            / f"month={month}"
        )

        partition_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = (
            partition_dir
            / (
                f"{dataset_name}_"
                f"{year}_{month}.json"
            )
        )

        with output_file.open(
            "w",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    json.dumps(record)
                    + "\n"
                )

        partition_record_count = len(
            records
        )

        records_partitioned += (
            partition_record_count
        )

        output_files.append(
            str(
                output_file.relative_to(
                    output_run_dir
                )
            )
        )

        print(
            f"{dataset_name}: wrote "
            f"{partition_record_count} records "
            f"to {output_file}"
        )

    print(
        f"{dataset_name}: "
        f"{records_partitioned} records "
        "partitioned"
    )

    if records_missing_date:
        print(
            f"{dataset_name}: skipped "
            f"{records_missing_date} records "
            f"without {date_field}"
        )

    return {
        "dataset": dataset_name,
        "input_file": str(
            input_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "date_field": date_field,
        "records_received": records_received,
        "records_partitioned": (
            records_partitioned
        ),
        "records_missing_date": (
            records_missing_date
        ),
        "partition_count": len(
            partitions
        ),
        "output_files": output_files,
    }


def write_manifest(
    run_id: str,
    validated_run_dir: Path,
    output_run_dir: Path,
    dataset_results: list[dict[str, Any]],
) -> Path:
    """
    Write metadata describing the partitioned
    files generated for this pipeline run.
    """

    manifest = {
        "run_id": run_id,
        "generated_at_utc": utc_now(),
        "source_directory": str(
            validated_run_dir.relative_to(
                PROJECT_ROOT
            )
        ),
        "output_directory": str(
            output_run_dir.relative_to(
                PROJECT_ROOT
            )
        ),
        "dataset_count": len(
            dataset_results
        ),
        "records_received": sum(
            result["records_received"]
            for result in dataset_results
        ),
        "records_partitioned": sum(
            result["records_partitioned"]
            for result in dataset_results
        ),
        "records_missing_date": sum(
            result["records_missing_date"]
            for result in dataset_results
        ),
        "datasets": dataset_results,
    }

    manifest_path = (
        output_run_dir
        / "partition_manifest.json"
    )

    with manifest_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            manifest,
            file,
            indent=2,
        )

    return manifest_path


def main() -> None:
    args = parse_args()

    validated_run_dir = (
        VALIDATED_DATA_DIR
        / args.run_id
    )

    output_run_dir = (
        PARTITIONED_DATA_DIR
        / args.run_id
    )

    output_raw_dir = (
        output_run_dir
        / "raw"
    )

    if not validated_run_dir.exists():
        print(
            "Validated run directory "
            "does not exist:"
        )
        print(validated_run_dir)
        print(
            "Run validate_data_contracts.py "
            "with the same --run-id first."
        )

        raise SystemExit(1)

    # Clear only this run's output.
    # Previous pipeline runs remain available.
    if output_run_dir.exists():
        shutil.rmtree(
            output_run_dir
        )

    output_raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        f"Pipeline run ID: {args.run_id}"
    )
    print(
        "Reading validated files from: "
        f"{validated_run_dir}"
    )
    print(
        "Writing partitioned files to: "
        f"{output_raw_dir}"
    )

    dataset_results: list[
        dict[str, Any]
    ] = []

    for (
        dataset_name,
        config,
    ) in DATASETS.items():
        result = write_partitioned_dataset(
            dataset_name=dataset_name,
            input_file=config["input_file"],
            date_field=config["date_field"],
            validated_run_dir=(
                validated_run_dir
            ),
            output_raw_dir=(
                output_raw_dir
            ),
            output_run_dir=(
                output_run_dir
            ),
        )

        dataset_results.append(
            result
        )

    manifest_path = write_manifest(
        run_id=args.run_id,
        validated_run_dir=(
            validated_run_dir
        ),
        output_run_dir=(
            output_run_dir
        ),
        dataset_results=(
            dataset_results
        ),
    )

    print(
        "Partition manifest written: "
        f"{manifest_path}"
    )
    print(
        "S3 partitioned files generated "
        "successfully."
    )


if __name__ == "__main__":
    main()