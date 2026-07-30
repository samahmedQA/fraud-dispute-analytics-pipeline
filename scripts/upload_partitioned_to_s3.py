from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARTITIONED_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "s3_partitioned"
)

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
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


def run_command(command: list[str]) -> None:
    """
    Execute an external command and stop if it fails.
    """

    print(
        "Command:",
        " ".join(command),
    )

    result = subprocess.run(
        command,
        check=False,
    )

    if result.returncode != 0:
        print(
            "Command failed with exit code "
            f"{result.returncode}"
        )

        raise SystemExit(
            result.returncode
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload one pipeline run's "
            "partitioned files to an S3 raw zone."
        )
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="Target S3 bucket name.",
    )

    parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help=(
            "Pipeline run ID associated "
            "with the partitioned data."
        ),
    )

    parser.add_argument(
        "--prefix",
        default="raw",
        help=(
            "Target S3 prefix. "
            "Default: raw"
        ),
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Actually upload files. "
            "Without this flag, the script "
            "runs as a dry run."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    local_partitioned_raw_dir = (
        PARTITIONED_DATA_DIR
        / args.run_id
        / "raw"
    )

    if not local_partitioned_raw_dir.exists():
        print(
            "Local partitioned raw directory "
            "does not exist:"
        )
        print(
            local_partitioned_raw_dir
        )
        print(
            "Run scripts/partition_data_for_s3.py "
            "with the same --run-id first."
        )

        raise SystemExit(1)

    if not local_partitioned_raw_dir.is_dir():
        print(
            "Local partitioned raw path "
            "is not a directory:"
        )
        print(
            local_partitioned_raw_dir
        )

        raise SystemExit(1)

    partitioned_files = [
        path
        for path in local_partitioned_raw_dir.rglob("*")
        if path.is_file()
    ]

    if not partitioned_files:
        print(
            "No partitioned files were found for "
            f"run ID {args.run_id}."
        )

        raise SystemExit(1)

    clean_prefix = args.prefix.strip("/")

    if clean_prefix:
        s3_uri = (
            f"s3://{args.bucket}/"
            f"{clean_prefix}/"
            f"run_id={args.run_id}/"
        )
    else:
        s3_uri = (
            f"s3://{args.bucket}/"
            f"run_id={args.run_id}/"
        )

    command = [
        "aws",
        "s3",
        "sync",
        str(local_partitioned_raw_dir),
        s3_uri,
    ]

    if not args.execute:
        command.append("--dryrun")

        print(
            "Running in DRY RUN mode. "
            "No files will be uploaded."
        )
    else:
        print(
            "Running in EXECUTE mode. "
            "Files will be uploaded to S3."
        )

    print(
        f"Pipeline run ID: {args.run_id}"
    )
    print(
        f"Files discovered: {len(partitioned_files)}"
    )
    print(
        f"Local source: {local_partitioned_raw_dir}"
    )
    print(
        f"S3 target: {s3_uri}"
    )

    run_command(command)

    print(
        "S3 upload step completed."
    )


if __name__ == "__main__":
    main()