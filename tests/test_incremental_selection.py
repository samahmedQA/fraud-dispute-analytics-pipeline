from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "incremental_selection.py"
SPEC = importlib.util.spec_from_file_location("incremental_selection", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
incremental = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(incremental)


RUN_1 = "20260824T170000Z_aaaaaaaa"
RUN_2 = "20260824T170001Z_bbbbbbbb"
RUN_3 = "20260824T170002Z_cccccccc"


def record(dataset: str, key: str, updated_at: str) -> dict[str, str]:
    key_fields = {
        "customers": "customer_id",
        "transactions": "transaction_id",
        "fraud_signals": "transaction_id",
        "disputes": "dispute_id",
        "chargeback_outcomes": "chargeback_id",
    }
    return {
        key_fields[dataset]: key,
        "updated_at": updated_at,
    }


def baseline_records(updated_at: str = "2026-08-20 12:00:00") -> dict[str, list[dict[str, str]]]:
    return {
        "customers": [record("customers", "CUS_0000001", updated_at)],
        "transactions": [record("transactions", "TXN_0000001", updated_at)],
        "fraud_signals": [record("fraud_signals", "TXN_0000001", updated_at)],
        "disputes": [record("disputes", "DISP_0000001", updated_at)],
        "chargeback_outcomes": [
            record("chargeback_outcomes", "CBK_0000001", updated_at)
        ],
    }


def write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def create_run(
    root: Path,
    run_id: str,
    records_by_dataset: dict[str, list[dict[str, str]]],
    *,
    contract_version: str = "v2",
) -> tuple[Path, Path]:
    validated_root = root / "validated"
    raw_root = root / "raw"
    validated_run = validated_root / run_id
    raw_run = raw_root / run_id
    validated_run.mkdir(parents=True)
    raw_run.mkdir(parents=True)

    (raw_run / "raw_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "contract_version": contract_version,
            }
        ),
        encoding="utf-8",
    )

    for dataset_name, config in incremental.DATASETS.items():
        write_jsonl(
            validated_run / config["input_file"],
            records_by_dataset[dataset_name],
        )

    return validated_root, raw_root


def select(
    root: Path,
    run_id: str,
    records_by_dataset: dict[str, list[dict[str, str]]],
    *,
    contract_version: str = "v2",
) -> Path:
    validated_root, raw_root = create_run(
        root,
        run_id,
        records_by_dataset,
        contract_version=contract_version,
    )
    return incremental.select_incremental_run(
        run_id,
        validated_data_dir=validated_root,
        raw_data_dir=raw_root,
        incremental_data_dir=root / "incremental",
        checkpoint_path=root / "state" / "checkpoint.json",
    )


def commit(root: Path, run_id: str) -> Path:
    return incremental.commit_candidate_checkpoint(
        run_id,
        incremental_data_dir=root / "incremental",
        checkpoint_path=root / "state" / "checkpoint.json",
    )


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_first_run_selects_every_record_without_committing_checkpoint(tmp_path: Path):
    rows = baseline_records()
    run_dir = select(tmp_path, RUN_1, rows)

    manifest = read_json(run_dir / "selection_manifest.json")
    assert manifest["records_received"] == 5
    assert manifest["records_selected"] == 5
    assert not (tmp_path / "state" / "checkpoint.json").exists()

    for dataset_name, config in incremental.DATASETS.items():
        assert read_jsonl(run_dir / config["input_file"]) == rows[dataset_name]


def test_commit_then_identical_snapshot_selects_nothing(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    commit(tmp_path, RUN_1)

    run_dir = select(tmp_path, RUN_2, rows)
    manifest = read_json(run_dir / "selection_manifest.json")

    assert manifest["records_selected"] == 0
    for config in incremental.DATASETS.values():
        assert read_jsonl(run_dir / config["input_file"]) == []


def test_later_updated_record_is_selected(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    commit(tmp_path, RUN_1)

    changed = baseline_records()
    changed["customers"][0] = record(
        "customers", "CUS_0000001", "2026-08-20 12:00:01"
    )
    run_dir = select(tmp_path, RUN_2, changed)

    assert read_jsonl(run_dir / "customers.json") == changed["customers"]
    manifest = read_json(run_dir / "selection_manifest.json")
    assert manifest["records_selected"] == 1


def test_new_record_at_exact_watermark_boundary_is_selected_once(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    commit(tmp_path, RUN_1)

    with_boundary = baseline_records()
    with_boundary["customers"].append(
        record("customers", "CUS_0000002", "2026-08-20 12:00:00")
    )

    run_dir = select(tmp_path, RUN_2, with_boundary)
    assert [row["customer_id"] for row in read_jsonl(run_dir / "customers.json")] == [
        "CUS_0000002"
    ]

    commit(tmp_path, RUN_2)
    run_dir_3 = select(tmp_path, RUN_3, with_boundary)
    assert read_jsonl(run_dir_3 / "customers.json") == []


def test_dataset_watermarks_advance_independently(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    commit(tmp_path, RUN_1)

    changed = baseline_records()
    changed["transactions"][0] = record(
        "transactions", "TXN_0000001", "2026-08-21 09:00:00"
    )
    run_dir = select(tmp_path, RUN_2, changed)
    candidate = read_json(run_dir / "candidate_checkpoint.json")

    assert candidate["datasets"]["transactions"]["watermark"] == "2026-08-21 09:00:00"
    assert candidate["datasets"]["customers"]["watermark"] == "2026-08-20 12:00:00"


def test_selection_never_mutates_committed_checkpoint(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    checkpoint = commit(tmp_path, RUN_1)
    before = checkpoint.read_bytes()

    changed = baseline_records("2026-08-22 10:00:00")
    select(tmp_path, RUN_2, changed)

    assert checkpoint.read_bytes() == before


def test_failed_selection_does_not_advance_checkpoint_or_publish_partial_run(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    checkpoint = commit(tmp_path, RUN_1)
    before = checkpoint.read_bytes()

    validated_root, raw_root = create_run(tmp_path / "broken", RUN_2, rows)
    (validated_root / RUN_2 / "transactions.json").unlink()

    with pytest.raises(FileNotFoundError):
        incremental.select_incremental_run(
            RUN_2,
            validated_data_dir=validated_root,
            raw_data_dir=raw_root,
            incremental_data_dir=tmp_path / "incremental",
            checkpoint_path=checkpoint,
        )

    assert checkpoint.read_bytes() == before
    assert not (tmp_path / "incremental" / RUN_2).exists()


def test_stale_candidate_commit_is_rejected(tmp_path: Path):
    rows = baseline_records()
    select(tmp_path, RUN_1, rows)
    commit(tmp_path, RUN_1)

    changed_a = baseline_records("2026-08-21 10:00:00")
    changed_b = baseline_records("2026-08-22 10:00:00")
    select(tmp_path, RUN_2, changed_a)
    select(tmp_path, RUN_3, changed_b)

    commit(tmp_path, RUN_2)

    with pytest.raises(incremental.IncrementalSelectionError, match="stale"):
        commit(tmp_path, RUN_3)


def test_malformed_checkpoint_fails_closed(tmp_path: Path):
    checkpoint = tmp_path / "state" / "checkpoint.json"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text("{not-json", encoding="utf-8")

    rows = baseline_records()
    validated_root, raw_root = create_run(tmp_path, RUN_1, rows)

    with pytest.raises(incremental.IncrementalSelectionError, match="not valid JSON"):
        incremental.select_incremental_run(
            RUN_1,
            validated_data_dir=validated_root,
            raw_data_dir=raw_root,
            incremental_data_dir=tmp_path / "incremental",
            checkpoint_path=checkpoint,
        )


def test_v1_snapshot_is_rejected(tmp_path: Path):
    rows = baseline_records()
    with pytest.raises(incremental.IncrementalSelectionError, match="requires a V2"):
        select(tmp_path, RUN_1, rows, contract_version="v1")


def test_invalid_updated_at_fails_closed(tmp_path: Path):
    rows = baseline_records()
    rows["customers"][0]["updated_at"] = "2026-99-99 12:00:00"

    with pytest.raises(incremental.IncrementalSelectionError, match="Invalid customers.updated_at"):
        select(tmp_path, RUN_1, rows)
