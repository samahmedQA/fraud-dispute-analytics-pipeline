import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_PARTITIONED_RAW_DIR = PROJECT_ROOT / "data" / "s3_partitioned" / "raw"


def run_command(command):
    print("Command:", " ".join(str(part) for part in command))
    result = subprocess.run(command)

    if result.returncode != 0:
        print(f"Command failed with exit code {result.returncode}")
        raise SystemExit(result.returncode)


def main():
    parser = argparse.ArgumentParser(
        description="Upload partitioned raw files to an S3 raw zone."
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="Target S3 bucket name.",
    )

    parser.add_argument(
        "--prefix",
        default="raw",
        help="Target S3 prefix. Default: raw",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually upload files. Without this flag, the script runs as a dry run.",
    )

    args = parser.parse_args()

    if not LOCAL_PARTITIONED_RAW_DIR.exists():
        print(f"Local partitioned raw directory does not exist: {LOCAL_PARTITIONED_RAW_DIR}")
        print("Run scripts/partition_data_for_s3.py first.")
        raise SystemExit(1)

    s3_uri = f"s3://{args.bucket}/{args.prefix.strip('/')}/"

    command = [
        "aws",
        "s3",
        "sync",
        str(LOCAL_PARTITIONED_RAW_DIR),
        s3_uri,
    ]

    if not args.execute:
        command.append("--dryrun")
        print("Running in DRY RUN mode. No files will be uploaded.")
    else:
        print("Running in EXECUTE mode. Files will be uploaded to S3.")

    print(f"Local source: {LOCAL_PARTITIONED_RAW_DIR}")
    print(f"S3 target: {s3_uri}")

    run_command(command)

    print("S3 upload step completed.")


if __name__ == "__main__":
    main()
