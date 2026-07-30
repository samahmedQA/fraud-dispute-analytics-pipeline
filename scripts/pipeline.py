from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "fraud_dispute_dbt"
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z_[0-9a-f]{8}$")


def validate_run_id(run_id: str) -> str:
    """Validate a pipeline run ID such as 20260730T190213Z_540d0c90."""

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )

    return run_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Operate the fraud dispute analytics pipeline through "
            "clear stage-oriented commands."
        )
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="COMMAND",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the end-to-end local pipeline.",
        description=(
            "Run generation, validation, partitioning, and optional "
            "S3, Snowflake, and dbt stages."
        ),
    )
    run_parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Use existing files in data/raw instead of regenerating data.",
    )
    run_parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Run the S3 upload stage after partitioning.",
    )
    run_parser.add_argument(
        "--bucket",
        default=os.getenv("FRAUD_DISPUTE_S3_BUCKET"),
        help="Target S3 bucket. Defaults to FRAUD_DISPUTE_S3_BUCKET.",
    )
    run_parser.add_argument(
        "--execute-s3",
        action="store_true",
        help="Perform the S3 upload instead of an upload dry run.",
    )
    run_parser.add_argument(
        "--load-snowflake",
        action="store_true",
        help="Run the Snowflake RAW loading stage.",
    )
    run_parser.add_argument(
        "--sql-file",
        default="sql/load_raw_from_s3.sql",
        help=(
            "Snowflake RAW load SQL file. "
            "Default: sql/load_raw_from_s3.sql."
        ),
    )
    run_parser.add_argument(
        "--execute-snowflake",
        action="store_true",
        help="Execute Snowflake SQL instead of a SQL dry run.",
    )
    run_parser.add_argument(
        "--dbt",
        action="store_true",
        help="Run dbt build after the earlier pipeline stages.",
    )
    run_parser.add_argument(
        "--dbt-target",
        default="dev",
        help="dbt target used by the end-to-end run. Default: dev.",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate data contracts for one pipeline run.",
    )
    validate_parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help="Existing pipeline run ID.",
    )

    partition_parser = subparsers.add_parser(
        "partition",
        help="Partition validated data for one pipeline run.",
    )
    partition_parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help="Existing pipeline run ID.",
    )

    upload_parser = subparsers.add_parser(
        "upload-s3",
        help="Upload and verify one partitioned run in S3.",
    )
    upload_parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help="Existing pipeline run ID.",
    )
    upload_parser.add_argument(
        "--bucket",
        default=os.getenv("FRAUD_DISPUTE_S3_BUCKET"),
        help="Target S3 bucket. Defaults to FRAUD_DISPUTE_S3_BUCKET.",
    )
    upload_parser.add_argument(
        "--prefix",
        default="raw",
        help="Target S3 prefix. Default: raw.",
    )
    upload_parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform the upload instead of a local dry run.",
    )
    upload_parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Intentionally replace an existing conflicting run prefix.",
    )

    snowflake_parser = subparsers.add_parser(
        "load-snowflake",
        help="Load one S3 run into Snowflake RAW tables.",
    )
    snowflake_parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help="Existing pipeline run ID.",
    )
    snowflake_parser.add_argument(
        "--sql-file",
        default="sql/load_raw_from_s3.sql",
        help=(
            "Snowflake RAW load SQL file. "
            "Default: sql/load_raw_from_s3.sql."
        ),
    )
    snowflake_parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute SQL instead of running a SQL dry run.",
    )

    dbt_parser = subparsers.add_parser(
        "dbt",
        help="Run dbt build for the analytics project.",
    )
    dbt_parser.add_argument(
        "--target",
        default="dev",
        help="dbt target. Default: dev.",
    )

    return parser


def validate_command_args(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> None:
    if args.command == "run":
        if args.execute_s3 and not args.upload_s3:
            parser.error("--execute-s3 requires --upload-s3")

        if args.upload_s3 and not args.bucket:
            parser.error(
                "--upload-s3 requires --bucket or "
                "FRAUD_DISPUTE_S3_BUCKET"
            )

        if args.execute_snowflake and not args.load_snowflake:
            parser.error(
                "--execute-snowflake requires --load-snowflake"
            )

    if args.command == "upload-s3" and not args.bucket:
        parser.error(
            "upload-s3 requires --bucket or FRAUD_DISPUTE_S3_BUCKET"
        )

    if (
        args.command == "upload-s3"
        and args.allow_overwrite
        and not args.execute
    ):
        parser.error("--allow-overwrite requires --execute")


def parse_cli_args(
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_command_args(parser, args)
    return args


def build_command(
    args: argparse.Namespace,
) -> tuple[list[str], Path]:
    if args.command == "run":
        command = [
            sys.executable,
            "scripts/run_pipeline.py",
        ]

        if args.skip_generate:
            command.append("--skip-generate")

        if args.upload_s3:
            command.append("--upload-s3")
            command.extend(["--s3-bucket", args.bucket])

        if args.execute_s3:
            command.append("--execute-s3-upload")

        if args.load_snowflake:
            command.append("--reload-snowflake")
            command.extend(
                [
                    "--snowflake-reload-sql",
                    args.sql_file,
                ]
            )

        if args.execute_snowflake:
            command.append("--execute-snowflake-reload")

        if args.dbt:
            command.append("--run-dbt")
            command.extend(["--dbt-target", args.dbt_target])

        return command, PROJECT_ROOT

    if args.command == "validate":
        return (
            [
                sys.executable,
                "scripts/validate_data_contracts.py",
                "--run-id",
                args.run_id,
            ],
            PROJECT_ROOT,
        )

    if args.command == "partition":
        return (
            [
                sys.executable,
                "scripts/partition_data_for_s3.py",
                "--run-id",
                args.run_id,
            ],
            PROJECT_ROOT,
        )

    if args.command == "upload-s3":
        command = [
            sys.executable,
            "scripts/upload_partitioned_to_s3.py",
            "--bucket",
            args.bucket,
            "--run-id",
            args.run_id,
            "--prefix",
            args.prefix,
        ]

        if args.execute:
            command.append("--execute")

        if args.allow_overwrite:
            command.append("--allow-overwrite")

        return command, PROJECT_ROOT

    if args.command == "load-snowflake":
        command = [
            sys.executable,
            "scripts/run_snowflake_sql.py",
            "--sql-file",
            args.sql_file,
            "--run-id",
            args.run_id,
        ]

        if args.execute:
            command.append("--execute")

        return command, PROJECT_ROOT

    if args.command == "dbt":
        return (
            [
                sys.executable,
                "-c",
                "from dbt.cli.main import cli; cli()",
                "build",
                "--target",
                args.target,
            ],
            DBT_PROJECT_DIR,
        )

    raise ValueError(f"Unsupported command: {args.command}")


def run_command(
    command: list[str],
    cwd: Path,
) -> int:
    print(f"Working directory: {cwd}")
    print(f"Command: {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
    )
    return result.returncode


def main(
    argv: Sequence[str] | None = None,
) -> None:
    args = parse_cli_args(argv)
    command, cwd = build_command(args)
    raise SystemExit(run_command(command, cwd))


if __name__ == "__main__":
    main()
