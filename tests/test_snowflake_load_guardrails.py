from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from scripts import run_snowflake_sql


TEST_RUN_ID = "20260730T040325Z_cd5bcec2"


def write_partitioned_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, dict]:
    partitioned_root = tmp_path / "s3_partitioned"
    run_dir = partitioned_root / TEST_RUN_ID
    datasets = []
    total_rows = 0

    monkeypatch.setattr(
        run_snowflake_sql,
        "PARTITIONED_ROOT",
        partitioned_root,
    )

    for index, dataset_name in enumerate(
        run_snowflake_sql.REQUIRED_DATASETS,
        start=1,
    ):
        relative_path = (
            Path("raw")
            / dataset_name
            / "year=2026"
            / "month=07"
            / f"{dataset_name}_2026_07.json"
        )
        output_file = run_dir / relative_path
        output_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        rows = [
            json.dumps(
                {
                    "dataset": dataset_name,
                    "row_number": row_number,
                }
            )
            for row_number in range(1, index + 1)
        ]
        output_file.write_text(
            "\n".join(rows) + "\n",
            encoding="utf-8",
        )

        datasets.append(
            {
                "dataset": dataset_name,
                "records_received": index,
                "records_partitioned": index,
                "records_missing_date": 0,
                "partition_count": 1,
                "output_files": [
                    str(relative_path).replace("/", "\\")
                ],
            }
        )
        total_rows += index

    manifest = {
        "run_id": TEST_RUN_ID,
        "dataset_count": len(datasets),
        "records_received": total_rows,
        "records_partitioned": total_rows,
        "records_missing_date": 0,
        "datasets": datasets,
    }

    manifest_path = run_dir / "partition_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    return manifest_path, manifest


def test_validate_run_id_accepts_expected_format():
    assert (
        run_snowflake_sql.validate_run_id(TEST_RUN_ID)
        == TEST_RUN_ID
    )

    with pytest.raises(argparse.ArgumentTypeError):
        run_snowflake_sql.validate_run_id(
            "../../wrong-run"
        )


def test_load_partition_manifest_validates_local_files(
    tmp_path,
    monkeypatch,
):
    _, source_manifest = write_partitioned_run(
        tmp_path,
        monkeypatch,
    )

    manifest = run_snowflake_sql.load_partition_manifest(
        TEST_RUN_ID
    )

    assert manifest["run_id"] == TEST_RUN_ID
    assert manifest["expected_total_files"] == 5
    assert (
        manifest["records_partitioned"]
        == source_manifest["records_partitioned"]
    )
    assert manifest["expected_rows"]["customers"] == 1
    assert (
        manifest["expected_rows"]["chargeback_outcomes"]
        == 5
    )


def test_manifest_rejects_missing_required_dataset(
    tmp_path,
    monkeypatch,
):
    manifest_path, manifest = write_partitioned_run(
        tmp_path,
        monkeypatch,
    )
    manifest["datasets"] = manifest["datasets"][:-1]
    manifest["dataset_count"] = 4
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Manifest dataset set is invalid",
    ):
        run_snowflake_sql.load_partition_manifest(
            TEST_RUN_ID
        )


def test_manifest_rejects_partition_row_mismatch(
    tmp_path,
    monkeypatch,
):
    manifest_path, manifest = write_partitioned_run(
        tmp_path,
        monkeypatch,
    )
    manifest["datasets"][0]["records_received"] = 2
    manifest["datasets"][0]["records_partitioned"] = 2
    manifest["records_received"] += 1
    manifest["records_partitioned"] += 1
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="local partition files contain 1 records",
    ):
        run_snowflake_sql.load_partition_manifest(
            TEST_RUN_ID
        )


def test_manifest_rejects_missing_partition_file(
    tmp_path,
    monkeypatch,
):
    _, manifest = write_partitioned_run(
        tmp_path,
        monkeypatch,
    )
    missing_path = (
        run_snowflake_sql.PARTITIONED_ROOT
        / TEST_RUN_ID
        / Path(
            manifest["datasets"][0]["output_files"][0]
            .replace("\\", "/")
        )
    )
    missing_path.unlink()

    with pytest.raises(
        FileNotFoundError,
        match="Manifest output file not found",
    ):
        run_snowflake_sql.load_partition_manifest(
            TEST_RUN_ID
        )


def test_render_sql_template_requires_and_substitutes_run_id():
    template = (
        "SELECT * FROM "
        "@S3_RAW_STAGE/run_id={{RUN_ID}}/customers/;"
    )

    with pytest.raises(
        ValueError,
        match="requires --run-id",
    ):
        run_snowflake_sql.render_sql_template(
            template,
            None,
        )

    rendered = run_snowflake_sql.render_sql_template(
        template,
        TEST_RUN_ID,
    )

    assert "{{RUN_ID}}" not in rendered
    assert f"run_id={TEST_RUN_ID}" in rendered


class FakeCursor:
    def __init__(self, results):
        self.results = iter(results)
        self.queries = []

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return next(self.results)


def guardrail_manifest():
    expected_rows = {
        dataset_name: index
        for index, dataset_name in enumerate(
            run_snowflake_sql.REQUIRED_DATASETS,
            start=1,
        )
    }
    expected_files = {
        dataset_name: 1
        for dataset_name in run_snowflake_sql.REQUIRED_DATASETS
    }

    return {
        "expected_rows": expected_rows,
        "expected_files": expected_files,
    }


def test_temporary_load_guardrail_accepts_exact_counts():
    manifest = guardrail_manifest()
    cursor = FakeCursor(
        [
            (manifest["expected_rows"][dataset], 1, 0, 0, 0)
            for dataset in run_snowflake_sql.REQUIRED_DATASETS
        ]
    )

    run_snowflake_sql.validate_temporary_load(
        cursor,
        TEST_RUN_ID,
        manifest,
    )

    assert len(cursor.queries) == 5
    assert TEST_RUN_ID in cursor.queries[0]


def test_temporary_load_guardrail_blocks_count_mismatch():
    manifest = guardrail_manifest()
    cursor = FakeCursor(
        [
            (999, 1, 0, 0, 0),
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="expected 1 rows, found 999",
    ):
        run_snowflake_sql.validate_temporary_load(
            cursor,
            TEST_RUN_ID,
            manifest,
        )


def test_guarded_dry_run_validates_manifest_without_connecting(
    tmp_path,
    monkeypatch,
    capsys,
):
    write_partitioned_run(
        tmp_path,
        monkeypatch,
    )
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    sql_file = sql_dir / "guarded.sql"
    sql_file.write_text(
        "COPY INTO TMP_RAW_CUSTOMERS "
        "FROM @S3_RAW_STAGE/run_id={{RUN_ID}}/customers/;\n"
        "SELECT '__PIPELINE_GUARDRAIL_VALIDATE_TEMP_LOAD__';\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_snowflake_sql,
        "PROJECT_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        run_snowflake_sql,
        "get_snowflake_connection",
        lambda: pytest.fail(
            "Dry run must not connect to Snowflake."
        ),
    )

    run_snowflake_sql.run_sql_file(
        "sql/guarded.sql",
        dry_run=True,
        run_id=TEST_RUN_ID,
    )

    output = capsys.readouterr().out
    assert f"Pipeline Run ID: {TEST_RUN_ID}" in output
    assert "Expected load: 15 records across 5 files" in output
    assert "DRY RUN mode" in output
    assert "Guardrail checkpoint" in output


def test_sql_and_pipeline_use_run_specific_guardrails():
    project_root = Path(__file__).resolve().parents[1]
    load_sql = (
        project_root
        / "sql"
        / "load_raw_from_s3.sql"
    ).read_text(encoding="utf-8")
    pipeline_code = (
        project_root
        / "scripts"
        / "run_pipeline.py"
    ).read_text(encoding="utf-8")

    assert "@S3_RAW_STAGE/run_id={{RUN_ID}}/customers/" in load_sql
    assert "METADATA$FILENAME" in load_sql
    assert "METADATA$FILE_ROW_NUMBER" in load_sql
    assert "__PIPELINE_GUARDRAIL_VALIDATE_TEMP_LOAD__" in load_sql
    assert '"--run-id",\n                run_id' in pipeline_code
