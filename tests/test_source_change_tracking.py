from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import scripts.generate_data as generator
from scripts.validate_data_contracts import (
    load_schema,
    resolve_contract_version,
    validate_schema,
    validate_updated_at_semantics,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATASET_CONFIG = {
    "customers": {
        "primary_key": "customer_id",
        "lifecycle_field": "created_at",
    },
    "transactions": {
        "primary_key": "transaction_id",
        "lifecycle_field": "transaction_timestamp",
    },
    "fraud_signals": {
        "primary_key": "transaction_id",
        "lifecycle_field": "score_timestamp",
    },
    "disputes": {
        "primary_key": "dispute_id",
        "lifecycle_field": "opened_date",
    },
    "chargeback_outcomes": {
        "primary_key": "chargeback_id",
        "lifecycle_field": "resolved_date",
    },
}


@pytest.fixture
def small_datasets(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """Generate a small deterministic V2 snapshot for focused tests."""
    monkeypatch.setattr(
        generator,
        "NUM_CUSTOMERS",
        30,
    )
    monkeypatch.setattr(
        generator,
        "NUM_TRANSACTIONS",
        200,
    )

    return generator.generate_datasets(seed=42)


@pytest.mark.parametrize(
    "dataset_name",
    DATASET_CONFIG,
)
def test_v2_contract_only_adds_updated_at(
    dataset_name: str,
) -> None:
    v1_path = (
        PROJECT_ROOT
        / "contracts"
        / "v1"
        / f"{dataset_name}.schema.json"
    )
    v2_path = (
        PROJECT_ROOT
        / "contracts"
        / "v2"
        / f"{dataset_name}.schema.json"
    )

    v1 = json.loads(
        v1_path.read_text(encoding="utf-8")
    )
    v2 = json.loads(
        v2_path.read_text(encoding="utf-8")
    )

    assert set(v2["required"]) == (
        set(v1["required"])
        | {"updated_at"}
    )
    assert set(v2["properties"]) == (
        set(v1["properties"])
        | {"updated_at"}
    )

    for field_name, field_schema in (
        v1["properties"].items()
    ):
        assert (
            v2["properties"][field_name]
            == field_schema
        )

    assert (
        v2["additionalProperties"]
        == v1["additionalProperties"]
    )
    assert (
        v2["properties"]["updated_at"]["format"]
        == "pipeline-date-time"
    )


def test_generated_updated_at_uses_existing_lifecycle_time(
    small_datasets: dict[str, Any],
) -> None:
    customers = small_datasets["customers"]
    transactions = small_datasets["transactions"]
    fraud_signals = small_datasets["fraud_signals"]
    disputes = small_datasets["disputes"]
    chargebacks = small_datasets[
        "chargeback_outcomes"
    ]

    assert (
        customers["updated_at"]
        == customers["created_at"]
        + " 00:00:00"
    ).all()

    assert (
        transactions["updated_at"]
        == transactions["transaction_timestamp"]
    ).all()

    assert (
        fraud_signals["updated_at"]
        == fraud_signals["score_timestamp"]
    ).all()

    assert (
        disputes["updated_at"]
        == disputes["opened_date"]
        + " 00:00:00"
    ).all()

    assert (
        chargebacks["updated_at"]
        == chargebacks["resolved_date"]
        + " 00:00:00"
    ).all()


def test_same_seed_produces_identical_source_hashes_across_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        generator,
        "NUM_CUSTOMERS",
        30,
    )
    monkeypatch.setattr(
        generator,
        "NUM_TRANSACTIONS",
        200,
    )

    run_a_dir = tmp_path / "run_a"
    run_b_dir = tmp_path / "run_b"
    run_a_dir.mkdir()
    run_b_dir.mkdir()

    manifest_a = generator.write_snapshot(
        output_dir=run_a_dir,
        run_id="20260818T170000Z_aaaaaaaa",
        seed=42,
        datasets=generator.generate_datasets(42),
    )
    manifest_b = generator.write_snapshot(
        output_dir=run_b_dir,
        run_id="20260818T170001Z_bbbbbbbb",
        seed=42,
        datasets=generator.generate_datasets(42),
    )

    assert manifest_a["contract_version"] == "v2"
    assert manifest_b["contract_version"] == "v2"

    for dataset_name in DATASET_CONFIG:
        assert (
            manifest_a["files"][dataset_name]["sha256"]
            == manifest_b["files"][dataset_name]["sha256"]
        )


def test_snapshot_rename_retries_transient_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()

    original_rename = Path.rename
    attempts = {"count": 0}
    sleep_calls: list[float] = []

    def flaky_rename(
        self: Path,
        target: Path,
    ) -> Path:
        if self == source:
            attempts["count"] += 1

            if attempts["count"] == 1:
                raise PermissionError(
                    "simulated transient Windows file lock"
                )

        return original_rename(self, target)

    monkeypatch.setattr(
        Path,
        "rename",
        flaky_rename,
    )
    monkeypatch.setattr(
        generator.time,
        "sleep",
        sleep_calls.append,
    )

    generator.rename_directory_with_retry(
        source,
        destination,
        attempts=3,
        base_delay_seconds=0.2,
    )

    assert attempts["count"] == 2
    assert sleep_calls == [0.2]
    assert destination.exists()
    assert not source.exists()




def test_historical_manifest_without_contract_version_uses_v1(
) -> None:
    assert resolve_contract_version({}) == "v1"


def test_v2_manifest_uses_v2_contracts() -> None:
    assert (
        resolve_contract_version(
            {"contract_version": "v2"}
        )
        == "v2"
    )


@pytest.mark.parametrize(
    (
        "dataset_name",
        "primary_key",
        "lifecycle_field",
        "lifecycle_value",
        "updated_at",
    ),
    [
        (
            "customers",
            "customer_id",
            "created_at",
            "2026-01-02",
            "2026-01-01 23:59:59",
        ),
        (
            "transactions",
            "transaction_id",
            "transaction_timestamp",
            "2026-01-02 12:00:00",
            "2026-01-02 11:59:59",
        ),
        (
            "fraud_signals",
            "transaction_id",
            "score_timestamp",
            "2026-01-02 12:00:00",
            "2026-01-02 11:59:59",
        ),
        (
            "disputes",
            "dispute_id",
            "opened_date",
            "2026-01-02",
            "2026-01-01 23:59:59",
        ),
        (
            "chargeback_outcomes",
            "chargeback_id",
            "resolved_date",
            "2026-01-02",
            "2026-01-01 23:59:59",
        ),
    ],
)
def test_updated_at_cannot_precede_lifecycle_timestamp(
    dataset_name: str,
    primary_key: str,
    lifecycle_field: str,
    lifecycle_value: str,
    updated_at: str,
) -> None:
    record = {
        primary_key: "TEST_ID",
        lifecycle_field: lifecycle_value,
        "updated_at": updated_at,
    }

    failures, invalid_indexes = (
        validate_updated_at_semantics(
            dataset_name=dataset_name,
            records=[(1, record)],
            primary_key=primary_key,
            lifecycle_field=lifecycle_field,
        )
    )

    assert invalid_indexes == {0}
    assert len(failures) == 1
    assert failures[0]["field"] == "updated_at"
    assert failures[0]["rule"] == (
        "updated_at_not_before_lifecycle"
    )
    assert failures[0]["severity"] == "hard_fail"


@pytest.mark.parametrize(
    "dataset_name",
    DATASET_CONFIG,
)
def test_v2_contract_rejects_missing_updated_at(
    dataset_name: str,
    small_datasets: dict[str, Any],
) -> None:
    config = DATASET_CONFIG[dataset_name]
    record = (
        small_datasets[dataset_name]
        .iloc[0]
        .to_dict()
    )
    record.pop("updated_at")

    schema = load_schema(
        PROJECT_ROOT
        / "contracts"
        / "v2"
        / f"{dataset_name}.schema.json"
    )

    failures, invalid_indexes = validate_schema(
        dataset_name=dataset_name,
        records=[(1, record)],
        schema=schema,
        primary_key=config["primary_key"],
    )

    assert invalid_indexes == {0}
    assert any(
        failure["rule"] == "required"
        and "updated_at" in failure["message"]
        for failure in failures
    )

def test_snapshot_rename_re_raises_persistent_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()

    attempts = {"count": 0}
    sleep_calls: list[float] = []

    def always_fail(
        self: Path,
        target: Path,
    ) -> Path:
        attempts["count"] += 1
        raise PermissionError(
            "persistent filesystem contention"
        )

    monkeypatch.setattr(
        Path,
        "rename",
        always_fail,
    )
    monkeypatch.setattr(
        generator.time,
        "sleep",
        sleep_calls.append,
    )

    with pytest.raises(
        PermissionError,
        match="persistent filesystem contention",
    ):
        generator.rename_directory_with_retry(
            source,
            destination,
            attempts=3,
            base_delay_seconds=0.2,
        )

    assert attempts["count"] == 3
    assert sleep_calls == [0.2, 0.4]
    assert source.exists()
    assert not destination.exists()
