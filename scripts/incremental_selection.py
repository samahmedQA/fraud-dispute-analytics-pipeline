from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALIDATED_DATA_DIR = PROJECT_ROOT / "data" / "validated"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
INCREMENTAL_DATA_DIR = PROJECT_ROOT / "data" / "incremental"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "incremental_state" / "checkpoint.json"

RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z_[0-9a-f]{8}$")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
CHECKPOINT_VERSION = "1.0"
SELECTION_VERSION = "1.0"
SUPPORTED_CONTRACT_VERSION = "v2"
ABSENT_CHECKPOINT_FINGERPRINT = "ABSENT"

DATASETS: dict[str, dict[str, str]] = {
    "customers": {
        "input_file": "customers.json",
        "primary_key": "customer_id",
    },
    "transactions": {
        "input_file": "transactions.json",
        "primary_key": "transaction_id",
    },
    "fraud_signals": {
        "input_file": "fraud_signals.json",
        "primary_key": "transaction_id",
    },
    "disputes": {
        "input_file": "disputes.json",
        "primary_key": "dispute_id",
    },
    "chargeback_outcomes": {
        "input_file": "chargeback_outcomes.json",
        "primary_key": "chargeback_id",
    },
}


class IncrementalSelectionError(RuntimeError):
    """Raised when incremental selection or checkpoint state is invalid."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )
    return run_id


def parse_timestamp(value: Any, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise IncrementalSelectionError(
            f"{field_name} must be a string in YYYY-MM-DD HH:MM:SS format."
        )
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError as error:
        raise IncrementalSelectionError(
            f"Invalid {field_name} value {value!r}; expected YYYY-MM-DD HH:MM:SS."
        ) from error


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def checkpoint_fingerprint(checkpoint_path: Path) -> str:
    if not checkpoint_path.exists():
        return ABSENT_CHECKPOINT_FINGERPRINT
    if not checkpoint_path.is_file():
        raise IncrementalSelectionError(
            f"Checkpoint path is not a file: {checkpoint_path}"
        )
    return sha256_bytes(checkpoint_path.read_bytes())


def empty_dataset_state() -> dict[str, Any]:
    return {"watermark": None, "boundary_keys": []}


def empty_checkpoint() -> dict[str, Any]:
    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "committed_from_run_id": None,
        "committed_at_utc": None,
        "datasets": {
            dataset_name: empty_dataset_state()
            for dataset_name in DATASETS
        },
    }


def _validate_dataset_state(dataset_name: str, state: Any) -> dict[str, Any]:
    if not isinstance(state, dict):
        raise IncrementalSelectionError(
            f"Checkpoint state for {dataset_name} must be an object."
        )

    watermark = state.get("watermark")
    boundary_keys = state.get("boundary_keys")

    if watermark is not None:
        parse_timestamp(watermark, field_name=f"{dataset_name}.watermark")

    if not isinstance(boundary_keys, list) or any(
        not isinstance(key, str) or not key
        for key in boundary_keys
    ):
        raise IncrementalSelectionError(
            f"Checkpoint boundary_keys for {dataset_name} must be a list of non-empty strings."
        )

    if len(boundary_keys) != len(set(boundary_keys)):
        raise IncrementalSelectionError(
            f"Checkpoint boundary_keys for {dataset_name} contains duplicates."
        )

    if watermark is None and boundary_keys:
        raise IncrementalSelectionError(
            f"Checkpoint boundary_keys for {dataset_name} must be empty when watermark is null."
        )

    return {
        "watermark": watermark,
        "boundary_keys": sorted(boundary_keys),
    }


def validate_checkpoint(checkpoint: Any) -> dict[str, Any]:
    if not isinstance(checkpoint, dict):
        raise IncrementalSelectionError("Checkpoint must be a JSON object.")

    if checkpoint.get("checkpoint_version") != CHECKPOINT_VERSION:
        raise IncrementalSelectionError(
            "Unsupported checkpoint_version: "
            f"{checkpoint.get('checkpoint_version')!r}."
        )

    datasets = checkpoint.get("datasets")
    if not isinstance(datasets, dict):
        raise IncrementalSelectionError("Checkpoint datasets must be an object.")

    if set(datasets) != set(DATASETS):
        missing = sorted(set(DATASETS) - set(datasets))
        unexpected = sorted(set(datasets) - set(DATASETS))
        raise IncrementalSelectionError(
            "Checkpoint dataset set is invalid. "
            f"Missing: {missing or 'none'}; Unexpected: {unexpected or 'none'}."
        )

    normalized = {
        "checkpoint_version": CHECKPOINT_VERSION,
        "committed_from_run_id": checkpoint.get("committed_from_run_id"),
        "committed_at_utc": checkpoint.get("committed_at_utc"),
        "datasets": {},
    }

    for dataset_name in DATASETS:
        normalized["datasets"][dataset_name] = _validate_dataset_state(
            dataset_name,
            datasets[dataset_name],
        )

    return normalized


def load_checkpoint(checkpoint_path: Path = CHECKPOINT_PATH) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return empty_checkpoint()

    try:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise IncrementalSelectionError(
            f"Checkpoint is not valid JSON: {checkpoint_path}"
        ) from error

    return validate_checkpoint(checkpoint)


def load_raw_manifest(raw_data_dir: Path, run_id: str) -> dict[str, Any]:
    manifest_path = raw_data_dir / run_id / "raw_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Raw manifest does not exist: {manifest_path}"
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise IncrementalSelectionError(
            f"Raw manifest is not valid JSON: {manifest_path}"
        ) from error

    if manifest.get("run_id") != run_id:
        raise IncrementalSelectionError(
            "Raw manifest run_id does not match requested run ID: "
            f"{manifest.get('run_id')!r} != {run_id!r}"
        )

    contract_version = manifest.get("contract_version")
    if contract_version != SUPPORTED_CONTRACT_VERSION:
        raise IncrementalSelectionError(
            "Incremental selection requires a V2 validated snapshot. "
            f"Manifest contract_version is {contract_version!r}."
        )

    return manifest


def load_json_lines(file_path: Path) -> Iterable[dict[str, Any]]:
    if not file_path.is_file():
        raise FileNotFoundError(f"Validated dataset does not exist: {file_path}")

    with file_path.open("r", encoding="utf-8") as file_handle:
        for line_number, line in enumerate(file_handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise IncrementalSelectionError(
                    f"Invalid JSON in {file_path} at line {line_number}: {error}"
                ) from error
            if not isinstance(record, dict):
                raise IncrementalSelectionError(
                    f"Expected a JSON object in {file_path} at line {line_number}."
                )
            yield record


def should_select(
    updated_at: str,
    primary_key: str,
    previous_state: dict[str, Any],
) -> bool:
    previous_watermark = previous_state["watermark"]
    if previous_watermark is None:
        return True

    updated_dt = parse_timestamp(updated_at, field_name="updated_at")
    watermark_dt = parse_timestamp(
        previous_watermark,
        field_name="checkpoint watermark",
    )

    if updated_dt > watermark_dt:
        return True
    if updated_dt < watermark_dt:
        return False
    return primary_key not in set(previous_state["boundary_keys"])


def candidate_state_for_dataset(
    records: list[dict[str, Any]],
    primary_key_field: str,
    previous_state: dict[str, Any],
) -> dict[str, Any]:
    previous_watermark = previous_state["watermark"]
    previous_boundary = set(previous_state["boundary_keys"])

    if not records:
        return {
            "watermark": previous_watermark,
            "boundary_keys": sorted(previous_boundary),
        }

    max_updated_at: str | None = None
    max_updated_dt: datetime | None = None
    boundary_keys: set[str] = set()

    for record in records:
        primary_key = record.get(primary_key_field)
        updated_at = record.get("updated_at")

        if not isinstance(primary_key, str) or not primary_key:
            raise IncrementalSelectionError(
                f"Record is missing non-empty primary key {primary_key_field!r}."
            )

        updated_dt = parse_timestamp(updated_at, field_name="updated_at")

        if max_updated_dt is None or updated_dt > max_updated_dt:
            max_updated_dt = updated_dt
            max_updated_at = updated_at
            boundary_keys = {primary_key}
        elif updated_dt == max_updated_dt:
            boundary_keys.add(primary_key)

    if previous_watermark is None:
        return {
            "watermark": max_updated_at,
            "boundary_keys": sorted(boundary_keys),
        }

    previous_dt = parse_timestamp(
        previous_watermark,
        field_name="checkpoint watermark",
    )
    assert max_updated_dt is not None

    if max_updated_dt > previous_dt:
        return {
            "watermark": max_updated_at,
            "boundary_keys": sorted(boundary_keys),
        }

    if max_updated_dt == previous_dt:
        return {
            "watermark": previous_watermark,
            "boundary_keys": sorted(previous_boundary | boundary_keys),
        }

    return {
        "watermark": previous_watermark,
        "boundary_keys": sorted(previous_boundary),
    }


def write_json_lines(path: Path, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as file_handle:
        for record in records:
            file_handle.write(json.dumps(record, sort_keys=True) + "\n")


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def select_incremental_run(
    run_id: str,
    *,
    validated_data_dir: Path = VALIDATED_DATA_DIR,
    raw_data_dir: Path = RAW_DATA_DIR,
    incremental_data_dir: Path = INCREMENTAL_DATA_DIR,
    checkpoint_path: Path = CHECKPOINT_PATH,
) -> Path:
    validate_run_id(run_id)
    load_raw_manifest(raw_data_dir, run_id)

    validated_run_dir = validated_data_dir / run_id
    if not validated_run_dir.is_dir():
        raise FileNotFoundError(
            f"Validated run directory does not exist: {validated_run_dir}"
        )

    base_checkpoint = load_checkpoint(checkpoint_path)
    base_fingerprint = checkpoint_fingerprint(checkpoint_path)

    final_run_dir = incremental_data_dir / run_id
    temp_run_dir = incremental_data_dir / f".{run_id}.tmp"

    if temp_run_dir.exists():
        shutil.rmtree(temp_run_dir)
    temp_run_dir.mkdir(parents=True, exist_ok=False)

    try:
        candidate_datasets: dict[str, dict[str, Any]] = {}
        dataset_results: list[dict[str, Any]] = []

        for dataset_name, config in DATASETS.items():
            input_path = validated_run_dir / config["input_file"]
            primary_key_field = config["primary_key"]
            records = list(load_json_lines(input_path))

            seen_keys: set[str] = set()
            selected_records: list[dict[str, Any]] = []
            previous_state = base_checkpoint["datasets"][dataset_name]

            for record in records:
                primary_key = record.get(primary_key_field)
                updated_at = record.get("updated_at")

                if not isinstance(primary_key, str) or not primary_key:
                    raise IncrementalSelectionError(
                        f"{dataset_name} record is missing non-empty "
                        f"{primary_key_field}."
                    )
                if primary_key in seen_keys:
                    raise IncrementalSelectionError(
                        f"Duplicate {primary_key_field} {primary_key!r} "
                        f"in validated {dataset_name} snapshot."
                    )
                seen_keys.add(primary_key)

                parse_timestamp(updated_at, field_name=f"{dataset_name}.updated_at")

                if should_select(updated_at, primary_key, previous_state):
                    selected_records.append(record)

            candidate_state = candidate_state_for_dataset(
                records,
                primary_key_field,
                previous_state,
            )
            candidate_datasets[dataset_name] = candidate_state

            output_file = temp_run_dir / config["input_file"]
            write_json_lines(output_file, selected_records)

            dataset_results.append(
                {
                    "dataset": dataset_name,
                    "input_file": str(input_path),
                    "output_file": config["input_file"],
                    "primary_key": primary_key_field,
                    "records_received": len(records),
                    "records_selected": len(selected_records),
                    "previous_watermark": previous_state["watermark"],
                    "candidate_watermark": candidate_state["watermark"],
                    "candidate_boundary_key_count": len(
                        candidate_state["boundary_keys"]
                    ),
                }
            )

        candidate_checkpoint = {
            "checkpoint_version": CHECKPOINT_VERSION,
            "candidate_for_run_id": run_id,
            "base_checkpoint_fingerprint": base_fingerprint,
            "datasets": candidate_datasets,
        }
        write_json(
            temp_run_dir / "candidate_checkpoint.json",
            candidate_checkpoint,
        )

        selection_manifest = {
            "selection_version": SELECTION_VERSION,
            "run_id": run_id,
            "contract_version": SUPPORTED_CONTRACT_VERSION,
            "generated_at_utc": utc_now(),
            "base_checkpoint_fingerprint": base_fingerprint,
            "dataset_count": len(DATASETS),
            "records_received": sum(
                item["records_received"] for item in dataset_results
            ),
            "records_selected": sum(
                item["records_selected"] for item in dataset_results
            ),
            "datasets": dataset_results,
        }
        write_json(
            temp_run_dir / "selection_manifest.json",
            selection_manifest,
        )

        if final_run_dir.exists():
            shutil.rmtree(final_run_dir)
        temp_run_dir.replace(final_run_dir)
        return final_run_dir

    except Exception:
        if temp_run_dir.exists():
            shutil.rmtree(temp_run_dir)
        raise


def commit_candidate_checkpoint(
    run_id: str,
    *,
    incremental_data_dir: Path = INCREMENTAL_DATA_DIR,
    checkpoint_path: Path = CHECKPOINT_PATH,
) -> Path:
    validate_run_id(run_id)

    candidate_path = (
        incremental_data_dir
        / run_id
        / "candidate_checkpoint.json"
    )
    if not candidate_path.is_file():
        raise FileNotFoundError(
            f"Candidate checkpoint does not exist: {candidate_path}"
        )

    try:
        candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise IncrementalSelectionError(
            f"Candidate checkpoint is not valid JSON: {candidate_path}"
        ) from error

    if candidate.get("candidate_for_run_id") != run_id:
        raise IncrementalSelectionError(
            "Candidate run_id does not match requested run ID."
        )

    expected_base = candidate.get("base_checkpoint_fingerprint")
    current_base = checkpoint_fingerprint(checkpoint_path)
    if expected_base != current_base:
        raise IncrementalSelectionError(
            "Candidate checkpoint is stale because committed checkpoint state "
            "has changed since selection. Re-run incremental selection."
        )

    datasets = candidate.get("datasets")
    checkpoint_to_write = validate_checkpoint(
        {
            "checkpoint_version": CHECKPOINT_VERSION,
            "committed_from_run_id": run_id,
            "committed_at_utc": utc_now(),
            "datasets": datasets,
        }
    )

    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    write_json(temp_path, checkpoint_to_write)
    temp_path.replace(checkpoint_path)
    return checkpoint_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Select new or changed V2 records using per-dataset committed watermarks."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    select_parser = subparsers.add_parser(
        "select",
        help="Create one run-scoped incremental selection and candidate checkpoint.",
    )
    select_parser.add_argument("--run-id", required=True, type=validate_run_id)

    commit_parser = subparsers.add_parser(
        "commit",
        help="Atomically commit the candidate checkpoint for a successful run.",
    )
    commit_parser.add_argument("--run-id", required=True, type=validate_run_id)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.command == "select":
            run_dir = select_incremental_run(args.run_id)
            manifest = json.loads(
                (run_dir / "selection_manifest.json").read_text(encoding="utf-8")
            )
            print(f"Pipeline run ID: {args.run_id}")
            print(f"Incremental output: {run_dir}")
            print(f"Records received: {manifest['records_received']}")
            print(f"Records selected: {manifest['records_selected']}")
            print("Committed checkpoint was not modified.")
        elif args.command == "commit":
            path = commit_candidate_checkpoint(args.run_id)
            print(f"Checkpoint committed: {path}")
    except (FileNotFoundError, IncrementalSelectionError) as error:
        print(f"Incremental selection failed: {error}")
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
