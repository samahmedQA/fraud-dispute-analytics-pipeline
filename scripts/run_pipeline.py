import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "fraud_dispute_dbt"


def run_step(step_name, command, cwd=PROJECT_ROOT):
    print("\n" + "=" * 80)
    print(f"Running step: {step_name}")
    print("=" * 80)
    print("Command:", " ".join(str(part) for part in command))

    result = subprocess.run(command, cwd=cwd)

    if result.returncode != 0:
        print("\n" + "!" * 80)
        print(f"Pipeline stopped. Step failed: {step_name}")
        print(f"Exit code: {result.returncode}")
        print("!" * 80)
        raise SystemExit(result.returncode)

    print(f"Step completed successfully: {step_name}")


def main():
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
        "--dbt-target",
        default="dev",
        help="dbt target to use when --run-dbt is provided. Default: dev.",
    )

    args = parser.parse_args()

    print("\nStarting local fraud dispute analytics pipeline")

    if not args.skip_generate:
        run_step(
            step_name="Generate synthetic raw data",
            command=[sys.executable, "scripts/generate_data.py"],
        )

    run_step(
        step_name="Validate data contracts",
        command=[sys.executable, "scripts/validate_data_contracts.py"],
    )

    run_step(
        step_name="Partition raw data for S3",
        command=[sys.executable, "scripts/partition_data_for_s3.py"],
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
            cwd=DBT_PROJECT_DIR,
        )

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
