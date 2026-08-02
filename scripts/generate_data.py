from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import shutil
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DATA_ROOT = PROJECT_ROOT / "data" / "raw"

DEFAULT_SEED = 42
BASE_DATE = datetime(2026, 7, 21)
GENERATOR_VERSION = "1.0.0"

NUM_TRANSACTIONS = 10_000
NUM_CUSTOMERS = 1_500

RUN_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z_[0-9a-f]{8}$"
)

CARD_NETWORKS = ["Visa", "Mastercard", "Pulse"]
MERCHANT_CATEGORIES = [
    "Retail",
    "Travel",
    "Food",
    "Electronics",
    "Gas",
    "Online",
]
TRANSACTION_STATUSES = [
    "Approved",
    "Declined",
    "Reversed",
]
DISPUTE_REASONS = [
    "Fraud",
    "Product Not Received",
    "Duplicate Charge",
    "Incorrect Amount",
]
DISPUTE_STATUSES = [
    "Opened",
    "Under Review",
    "Resolved",
    "Denied",
]
CHARGEBACK_OUTCOMES = [
    "Won",
    "Lost",
    "Pending",
]


def validate_run_id(run_id: str) -> str:
    """Validate the public pipeline run-ID format."""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match "
            "YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )

    return run_id


def parse_args() -> argparse.Namespace:
    """Parse generator command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate one immutable synthetic fraud and dispute "
            "raw-data snapshot."
        )
    )

    parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help=(
            "Pipeline run ID that owns the generated raw snapshot."
        ),
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=(
            "Deterministic random seed. "
            f"Default: {DEFAULT_SEED}."
        ),
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_RAW_DATA_ROOT,
        help=(
            "Root directory that will contain <run_id>/. "
            "Defaults to data/raw."
        ),
    )

    return parser.parse_args()


def calculate_sha256(file_path: Path) -> str:
    """Calculate a file's SHA-256 digest without loading it all at once."""
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def count_json_lines(file_path: Path) -> int:
    """Count nonblank JSONL records in a generated dataset."""
    with file_path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def verify_existing_snapshot(
    output_dir: Path,
    run_id: str,
    seed: int,
) -> dict[str, Any]:
    """
    Verify an existing snapshot and return its manifest.

    A valid snapshot is treated as an idempotent success. An incomplete or
    conflicting snapshot is rejected rather than overwritten.
    """
    manifest_path = output_dir / "raw_manifest.json"

    if not manifest_path.is_file():
        raise SystemExit(
            "Raw snapshot directory already exists but its manifest "
            f"is missing: {manifest_path}"
        )

    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as error:
        raise SystemExit(
            "Raw snapshot manifest is not valid JSON: "
            f"{manifest_path}: {error}"
        ) from error

    if manifest.get("run_id") != run_id:
        raise SystemExit(
            "Existing raw snapshot has a conflicting run ID: "
            f"{manifest.get('run_id')!r}"
        )

    if manifest.get("seed") != seed:
        raise SystemExit(
            "Existing raw snapshot was generated with seed "
            f"{manifest.get('seed')!r}, not requested seed {seed}."
        )

    files = manifest.get("files")

    if not isinstance(files, dict) or not files:
        raise SystemExit(
            "Existing raw manifest does not contain file metadata."
        )

    for dataset_name, metadata in files.items():
        if not isinstance(metadata, dict):
            raise SystemExit(
                "Invalid manifest metadata for dataset "
                f"{dataset_name!r}."
            )

        file_name = metadata.get("file_name")

        if not isinstance(file_name, str):
            raise SystemExit(
                "Manifest file name is missing for dataset "
                f"{dataset_name!r}."
            )

        file_path = output_dir / file_name

        if not file_path.is_file():
            raise SystemExit(
                "Existing raw snapshot is incomplete. Missing file: "
                f"{file_path}"
            )

        actual_size = file_path.stat().st_size
        expected_size = metadata.get("file_size_bytes")

        if actual_size != expected_size:
            raise SystemExit(
                "Existing raw file size does not match the manifest: "
                f"{file_path}"
            )

        actual_hash = calculate_sha256(file_path)
        expected_hash = metadata.get("sha256")

        if actual_hash != expected_hash:
            raise SystemExit(
                "Existing raw file hash does not match the manifest: "
                f"{file_path}"
            )

        actual_count = count_json_lines(file_path)
        expected_count = metadata.get("record_count")

        if actual_count != expected_count:
            raise SystemExit(
                "Existing raw record count does not match the manifest: "
                f"{file_path}"
            )

    return manifest


def random_date(
    fake: Faker,
    days_back: int = 365,
) -> datetime:
    """Generate a deterministic fake datetime in a fixed date window."""
    end_date = BASE_DATE
    start_date = end_date - timedelta(days=days_back)

    return fake.date_time_between(
        start_date=start_date,
        end_date=end_date,
    )


def generate_datasets(
    seed: int,
) -> dict[str, pd.DataFrame]:
    """Generate all five deterministic synthetic datasets."""
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)

    fake = Faker()
    fake.seed_instance(seed)

    customers: list[dict[str, Any]] = []

    for index in range(1, NUM_CUSTOMERS + 1):
        customers.append(
            {
                "customer_id": f"CUST_{index:06d}",
                "account_id": f"ACCT_{index:06d}",
                "customer_age": random.randint(18, 75),
                "account_status": random.choice(
                    ["Active", "Suspended", "Closed"]
                ),
                "state": fake.state_abbr(),
                "created_at": random_date(
                    fake,
                    1000,
                ).strftime("%Y-%m-%d"),
            }
        )

    customers_df = pd.DataFrame(customers)

    transactions: list[dict[str, Any]] = []

    for index in range(1, NUM_TRANSACTIONS + 1):
        customer = customers_df.sample(1).iloc[0]
        transaction_timestamp = random_date(fake, 365)

        transactions.append(
            {
                "transaction_id": f"TXN_{index:08d}",
                "customer_id": customer["customer_id"],
                "account_id": customer["account_id"],
                "merchant_id": (
                    f"MERCH_{random.randint(1, 500):05d}"
                ),
                "transaction_amount": round(
                    random.uniform(5, 2000),
                    2,
                ),
                "transaction_timestamp": (
                    transaction_timestamp.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                ),
                "transaction_status": random.choice(
                    TRANSACTION_STATUSES
                ),
                "card_network": random.choice(CARD_NETWORKS),
                "merchant_category": random.choice(
                    MERCHANT_CATEGORIES
                ),
                "country": "US",
            }
        )

    transactions_df = pd.DataFrame(transactions)

    fraud_signals: list[dict[str, Any]] = []

    for _, row in transactions_df.iterrows():
        fraud_score = round(np.random.beta(2, 8), 4)

        if fraud_score >= 0.75:
            risk_level = "High"
            rule_triggered = random.choice(
                [
                    "Velocity Check",
                    "High Amount",
                    "Device Mismatch",
                    "Geo Anomaly",
                ]
            )
        elif fraud_score >= 0.40:
            risk_level = "Medium"
            rule_triggered = random.choice(
                [
                    "Unusual Merchant",
                    "Amount Spike",
                    "New Device",
                ]
            )
        else:
            risk_level = "Low"
            rule_triggered = "None"

        fraud_signals.append(
            {
                "transaction_id": row["transaction_id"],
                "fraud_score": fraud_score,
                "risk_level": risk_level,
                "rule_triggered": rule_triggered,
                "device_risk_score": round(
                    random.uniform(0, 1),
                    4,
                ),
                "velocity_count": random.randint(0, 12),
                "model_version": random.choice(
                    ["v1.0", "v1.1", "v1.2"]
                ),
                "score_timestamp": row[
                    "transaction_timestamp"
                ],
            }
        )

    fraud_signals_df = pd.DataFrame(fraud_signals)

    disputed_transactions = transactions_df.sample(
        frac=0.12,
        random_state=42,
    )
    disputes: list[dict[str, Any]] = []

    for index, (_, row) in enumerate(
        disputed_transactions.iterrows(),
        start=1,
    ):
        opened_date = pd.to_datetime(
            row["transaction_timestamp"]
        ) + timedelta(days=random.randint(1, 45))

        disputes.append(
            {
                "dispute_id": f"DISP_{index:07d}",
                "claim_id": f"CLM_{index:07d}",
                "transaction_id": row["transaction_id"],
                "dispute_reason": random.choice(
                    DISPUTE_REASONS
                ),
                "dispute_amount": row[
                    "transaction_amount"
                ],
                "dispute_status": random.choice(
                    DISPUTE_STATUSES
                ),
                "opened_date": opened_date.strftime(
                    "%Y-%m-%d"
                ),
                "card_network": row["card_network"],
            }
        )

    disputes_df = pd.DataFrame(disputes)

    chargeback_disputes = disputes_df.sample(
        frac=0.70,
        random_state=24,
    )
    chargebacks: list[dict[str, Any]] = []

    for index, (_, row) in enumerate(
        chargeback_disputes.iterrows(),
        start=1,
    ):
        resolved_date = pd.to_datetime(
            row["opened_date"]
        ) + timedelta(days=random.randint(10, 90))
        outcome = random.choice(CHARGEBACK_OUTCOMES)

        if outcome == "Won":
            final_amount = row["dispute_amount"]
            win_loss_flag = "Win"
        elif outcome == "Lost":
            final_amount = 0
            win_loss_flag = "Loss"
        else:
            final_amount = 0
            win_loss_flag = "Pending"

        chargebacks.append(
            {
                "chargeback_id": f"CBK_{index:07d}",
                "dispute_id": row["dispute_id"],
                "outcome": outcome,
                "win_loss_flag": win_loss_flag,
                "final_amount": final_amount,
                "resolved_date": resolved_date.strftime(
                    "%Y-%m-%d"
                ),
                "representment_required": random.choice(
                    [True, False]
                ),
            }
        )

    chargebacks_df = pd.DataFrame(chargebacks)

    return {
        "customers": customers_df,
        "transactions": transactions_df,
        "fraud_signals": fraud_signals_df,
        "disputes": disputes_df,
        "chargeback_outcomes": chargebacks_df,
    }


def write_snapshot(
    output_dir: Path,
    run_id: str,
    seed: int,
    datasets: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """Write JSONL datasets and their raw manifest."""
    file_names = {
        "customers": "customers.json",
        "transactions": "transactions.json",
        "fraud_signals": "fraud_signals.json",
        "disputes": "disputes.json",
        "chargeback_outcomes": "chargeback_outcomes.json",
    }

    for dataset_name, dataframe in datasets.items():
        dataframe.to_json(
            output_dir / file_names[dataset_name],
            orient="records",
            lines=True,
        )

    manifest_files: dict[str, dict[str, Any]] = {}

    for dataset_name, dataframe in datasets.items():
        file_name = file_names[dataset_name]
        file_path = output_dir / file_name

        manifest_files[dataset_name] = {
            "file_name": file_name,
            "record_count": len(dataframe),
            "file_size_bytes": file_path.stat().st_size,
            "sha256": calculate_sha256(file_path),
        }

    raw_manifest: dict[str, Any] = {
        "manifest_version": "1.0",
        "run_id": run_id,
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "base_date": BASE_DATE.date().isoformat(),
        "generated_at_utc": (
            datetime.now(UTC)
            .isoformat()
            .replace("+00:00", "Z")
        ),
        "dataset_count": len(manifest_files),
        "total_record_count": sum(
            metadata["record_count"]
            for metadata in manifest_files.values()
        ),
        "files": manifest_files,
    }

    manifest_path = output_dir / "raw_manifest.json"

    with manifest_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file:
        json.dump(
            raw_manifest,
            file,
            indent=2,
            sort_keys=True,
        )
        file.write("\n")

    return raw_manifest


def create_snapshot(
    raw_data_root: Path,
    run_id: str,
    seed: int,
) -> tuple[Path, dict[str, Any], bool]:
    """
    Create one immutable raw snapshot.

    Returns the final directory, manifest, and a flag indicating whether
    the snapshot already existed and was verified.
    """
    raw_data_root.mkdir(parents=True, exist_ok=True)
    output_dir = raw_data_root / run_id

    if output_dir.exists():
        manifest = verify_existing_snapshot(
            output_dir=output_dir,
            run_id=run_id,
            seed=seed,
        )
        return output_dir, manifest, True

    temporary_dir = raw_data_root / (
        f".{run_id}.{uuid.uuid4().hex}.tmp"
    )
    temporary_dir.mkdir(parents=False, exist_ok=False)

    try:
        datasets = generate_datasets(seed)
        manifest = write_snapshot(
            output_dir=temporary_dir,
            run_id=run_id,
            seed=seed,
            datasets=datasets,
        )

        try:
            temporary_dir.rename(output_dir)
        except OSError:
            if not output_dir.exists():
                raise

            manifest = verify_existing_snapshot(
                output_dir=output_dir,
                run_id=run_id,
                seed=seed,
            )
            return output_dir, manifest, True

        return output_dir, manifest, False
    finally:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)


def main() -> None:
    """Generate or verify one run-scoped raw snapshot."""
    args = parse_args()

    output_dir, manifest, already_existed = create_snapshot(
        raw_data_root=args.output_root.resolve(),
        run_id=args.run_id,
        seed=args.seed,
    )

    if already_existed:
        print(
            "Existing immutable raw snapshot verified successfully."
        )
    else:
        print(
            "Synthetic fintech JSON data generated successfully."
        )

    print(f"Pipeline Run ID: {args.run_id}")
    print(f"Seed: {args.seed}")
    print(f"Raw output directory: {output_dir}")
    print(
        "Raw manifest: "
        f"{output_dir / 'raw_manifest.json'}"
    )
    print(
        "Total records: "
        f"{manifest['total_record_count']}"
    )

    for dataset_name, metadata in manifest["files"].items():
        print(
            f"{dataset_name}: "
            f"{metadata['record_count']}"
        )


if __name__ == "__main__":
    main()
