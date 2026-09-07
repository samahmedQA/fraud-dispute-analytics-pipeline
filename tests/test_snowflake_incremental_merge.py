import json
from pathlib import Path

import pytest

from scripts import run_snowflake_sql


RUN_ID = "20260827T190000Z_aaaaaaaa"


def write_partition_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows_by_dataset: dict[str, int],
) -> None:
    partitioned_root = tmp_path / "s3_partitioned"
    run_dir = partitioned_root / RUN_ID
    run_dir.mkdir(parents=True)

    monkeypatch.setattr(
        run_snowflake_sql,
        "PARTITIONED_ROOT",
        partitioned_root,
    )

    datasets = []
    total_rows = 0

    for dataset_name in run_snowflake_sql.REQUIRED_DATASETS:
        row_count = rows_by_dataset.get(dataset_name, 0)
        output_files = []

        if row_count:
            relative_path = (
                Path("raw")
                / dataset_name
                / "year=2026"
                / "month=08"
                / f"{dataset_name}_2026_08.json"
            )

            local_file = run_dir / relative_path
            local_file.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            local_file.write_text(
                "".join(
                    json.dumps({"row": index}) + "\n"
                    for index in range(row_count)
                ),
                encoding="utf-8",
            )

            output_files.append(relative_path.as_posix())

        datasets.append(
            {
                "dataset": dataset_name,
                "records_received": row_count,
                "records_partitioned": row_count,
                "records_missing_date": 0,
                "partition_count": len(output_files),
                "output_files": output_files,
            }
        )

        total_rows += row_count

    manifest = {
        "run_id": RUN_ID,
        "dataset_count": len(
            run_snowflake_sql.REQUIRED_DATASETS
        ),
        "records_received": total_rows,
        "records_partitioned": total_rows,
        "records_missing_date": 0,
        "datasets": datasets,
    }

    (run_dir / "partition_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )


def test_incremental_manifest_accepts_sparse_batch(
    tmp_path,
    monkeypatch,
):
    write_partition_manifest(
        tmp_path,
        monkeypatch,
        {"customers": 1},
    )

    manifest = run_snowflake_sql.load_partition_manifest(
        RUN_ID,
        load_mode="incremental",
    )

    assert manifest["expected_rows"]["customers"] == 1
    assert manifest["expected_files"]["customers"] == 1

    for dataset_name in (
        "transactions",
        "fraud_signals",
        "disputes",
        "chargeback_outcomes",
    ):
        assert manifest["expected_rows"][dataset_name] == 0
        assert manifest["expected_files"][dataset_name] == 0


def test_incremental_manifest_accepts_zero_change_batch(
    tmp_path,
    monkeypatch,
):
    write_partition_manifest(
        tmp_path,
        monkeypatch,
        {},
    )

    manifest = run_snowflake_sql.load_partition_manifest(
        RUN_ID,
        load_mode="incremental",
    )

    assert manifest["records_partitioned"] == 0
    assert manifest["expected_total_files"] == 0
    assert all(
        value == 0
        for value in manifest["expected_rows"].values()
    )


def test_full_mode_stays_strict_for_zero_row_dataset(
    tmp_path,
    monkeypatch,
):
    write_partition_manifest(
        tmp_path,
        monkeypatch,
        {},
    )

    with pytest.raises(
        ValueError,
        match="must be a positive integer",
    ):
        run_snowflake_sql.load_partition_manifest(
            RUN_ID,
            load_mode="full",
        )


class FakeCursor:
    def __init__(self, results):
        self.results = iter(results)
        self.queries = []

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return next(self.results)


def incremental_manifest(expected_customers=0):
    expected_rows = {
        dataset: 0
        for dataset in run_snowflake_sql.REQUIRED_DATASETS
    }
    expected_files = {
        dataset: 0
        for dataset in run_snowflake_sql.REQUIRED_DATASETS
    }

    expected_rows["customers"] = expected_customers
    expected_files["customers"] = (
        1 if expected_customers else 0
    )

    return {
        "expected_rows": expected_rows,
        "expected_files": expected_files,
    }


def test_incremental_guardrail_accepts_zero_row_tables():
    results = []

    for _ in run_snowflake_sql.REQUIRED_DATASETS:
        results.extend(
            [
                (0, 0, 0, 0, 0),
                (0, 0),
                (0,),
            ]
        )

    cursor = FakeCursor(results)

    run_snowflake_sql.validate_temporary_load(
        cursor,
        RUN_ID,
        incremental_manifest(),
        load_mode="incremental",
    )

    assert len(cursor.queries) == 15


def test_incremental_guardrail_rejects_bad_change_metadata():
    cursor = FakeCursor(
        [
            (1, 1, 0, 0, 0),
            (0, 1),
            (1,),
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="missing or invalid updated_at",
    ) as error:
        run_snowflake_sql.validate_temporary_load(
            cursor,
            RUN_ID,
            incremental_manifest(expected_customers=1),
            load_mode="incremental",
        )

    assert "duplicate customer_id values" in str(error.value)


def test_merge_sql_preserves_incremental_idempotency_rules():
    sql = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "merge_raw_from_s3.sql"
    ).read_text(encoding="utf-8")

    assert sql.count("MERGE INTO RAW_") == 5
    assert "DELETE FROM RAW_" not in sql

    for primary_key in (
        "customer_id",
        "transaction_id",
        "dispute_id",
        "chargeback_id",
    ):
        assert f"raw_record:{primary_key}" in sql

    assert "WHEN MATCHED AND" in sql
    assert "WHEN NOT MATCHED THEN" in sql
    assert "raw_record:updated_at" in sql
    assert "TRY_TO_TIMESTAMP_NTZ" in sql

def test_incremental_guardrail_coalesces_empty_count_if_results():
    results = []

    for _ in run_snowflake_sql.REQUIRED_DATASETS:
        results.extend(
            [
                (0, 0, 0, 0, 0),
                (0, 0),
                (0,),
            ]
        )

    cursor = FakeCursor(results)

    run_snowflake_sql.validate_temporary_load(
        cursor,
        RUN_ID,
        incremental_manifest(),
        load_mode="incremental",
    )

    sql = "\n".join(cursor.queries)

    # Five COUNT_IF checks per dataset must coalesce NULL -> 0
    # so sparse incremental datasets with zero rows are valid.
    expected_coalesce_count = (
        len(run_snowflake_sql.REQUIRED_DATASETS) * 5
    )

    assert sql.count("COALESCE(") == expected_coalesce_count
    assert "COUNT_IF(" in sql
