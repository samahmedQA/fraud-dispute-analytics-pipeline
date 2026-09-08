from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import partition_data_for_s3
from scripts import pipeline
from scripts.upload_partitioned_to_s3 import load_local_batch


RUN_ID = "20260825T000000Z_abcdef12"


def create_partitioned_incremental_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    selected_counts: dict[str, int],
) -> tuple[Path, dict]:
    project_root = tmp_path / "project"
    source_run_dir = (
        project_root
        / "data"
        / "incremental"
        / RUN_ID
    )
    output_run_dir = (
        project_root
        / "data"
        / "s3_partitioned"
        / RUN_ID
    )
    output_raw_dir = output_run_dir / "raw"

    source_run_dir.mkdir(parents=True)
    output_raw_dir.mkdir(parents=True)

    monkeypatch.setattr(
        partition_data_for_s3,
        "PROJECT_ROOT",
        project_root,
    )

    for dataset_name, config in partition_data_for_s3.DATASETS.items():
        count = selected_counts.get(dataset_name, 0)

        records = [
            {
                config["date_field"]: "2026-07-15 12:00:00",
                "test_record": index,
            }
            for index in range(count)
        ]

        input_path = source_run_dir / config["input_file"]
        input_path.write_text(
            "".join(
                json.dumps(record) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )

    dataset_results = []

    for dataset_name, config in partition_data_for_s3.DATASETS.items():
        dataset_results.append(
            partition_data_for_s3.write_partitioned_dataset(
                dataset_name=dataset_name,
                input_file=config["input_file"],
                date_field=config["date_field"],
                validated_run_dir=source_run_dir,
                output_raw_dir=output_raw_dir,
                output_run_dir=output_run_dir,
            )
        )

    manifest_path = partition_data_for_s3.write_manifest(
        run_id=RUN_ID,
        validated_run_dir=source_run_dir,
        output_run_dir=output_run_dir,
        dataset_results=dataset_results,
    )

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    return project_root, manifest


def test_sparse_incremental_batch_allows_zero_row_datasets(
    tmp_path,
    monkeypatch,
):
    project_root, manifest = create_partitioned_incremental_run(
        tmp_path,
        monkeypatch,
        {"customers": 1},
    )

    assert manifest["records_received"] == 1
    assert manifest["records_partitioned"] == 1

    by_dataset = {
        item["dataset"]: item
        for item in manifest["datasets"]
    }

    assert by_dataset["customers"]["records_partitioned"] == 1
    assert len(by_dataset["customers"]["output_files"]) == 1

    for dataset_name in (
        "transactions",
        "fraud_signals",
        "disputes",
        "chargeback_outcomes",
    ):
        assert by_dataset[dataset_name]["records_partitioned"] == 0
        assert by_dataset[dataset_name]["partition_count"] == 0
        assert by_dataset[dataset_name]["output_files"] == []

    batch = load_local_batch(
        run_id=RUN_ID,
        partitioned_data_dir=(
            project_root
            / "data"
            / "s3_partitioned"
        ),
    )

    assert batch.records_partitioned == 1
    assert batch.file_count == 1
    assert batch.files[0].relative_key.startswith("customers/")


def test_all_zero_incremental_batch_is_valid(
    tmp_path,
    monkeypatch,
):
    project_root, manifest = create_partitioned_incremental_run(
        tmp_path,
        monkeypatch,
        {},
    )

    assert manifest["records_received"] == 0
    assert manifest["records_partitioned"] == 0

    for dataset in manifest["datasets"]:
        assert dataset["records_partitioned"] == 0
        assert dataset["partition_count"] == 0
        assert dataset["output_files"] == []

    batch = load_local_batch(
        run_id=RUN_ID,
        partitioned_data_dir=(
            project_root
            / "data"
            / "s3_partitioned"
        ),
    )

    assert batch.records_partitioned == 0
    assert batch.file_count == 0


def test_repartition_same_run_preserves_manifest_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "project"
    incremental_root = (
        project_root
        / "data"
        / "incremental"
    )
    partitioned_root = (
        project_root
        / "data"
        / "s3_partitioned"
    )
    source_run_dir = incremental_root / RUN_ID
    source_run_dir.mkdir(parents=True)

    for config in partition_data_for_s3.DATASETS.values():
        (
            source_run_dir
            / config["input_file"]
        ).write_text(
            "",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        partition_data_for_s3,
        "PROJECT_ROOT",
        project_root,
    )
    monkeypatch.setattr(
        partition_data_for_s3,
        "INCREMENTAL_DATA_DIR",
        incremental_root,
    )
    monkeypatch.setattr(
        partition_data_for_s3,
        "PARTITIONED_DATA_DIR",
        partitioned_root,
    )

    timestamps = iter(
        [
            "2026-09-07T18:00:00Z",
            "2026-09-07T18:01:00Z",
        ]
    )

    monkeypatch.setattr(
        partition_data_for_s3,
        "utc_now",
        lambda: next(timestamps),
    )

    args = type(
        "Args",
        (),
        {
            "source": "incremental",
            "run_id": RUN_ID,
        },
    )()

    monkeypatch.setattr(
        partition_data_for_s3,
        "parse_args",
        lambda: args,
    )

    partition_data_for_s3.main()

    manifest_path = (
        partitioned_root
        / RUN_ID
        / "partition_manifest.json"
    )
    first_manifest = manifest_path.read_bytes()

    partition_data_for_s3.main()
    second_manifest = manifest_path.read_bytes()

    assert first_manifest == second_manifest

    manifest = json.loads(
        second_manifest.decode("utf-8")
    )
    assert (
        manifest["generated_at_utc"]
        == "2026-09-07T18:00:00Z"
    )




def test_nonzero_dataset_still_requires_output_file(
    tmp_path,
    monkeypatch,
):
    project_root, manifest = create_partitioned_incremental_run(
        tmp_path,
        monkeypatch,
        {"customers": 1},
    )

    customer = next(
        item
        for item in manifest["datasets"]
        if item["dataset"] == "customers"
    )

    customer["partition_count"] = 0
    customer["output_files"] = []

    manifest_path = (
        project_root
        / "data"
        / "s3_partitioned"
        / RUN_ID
        / "partition_manifest.json"
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="has no output files",
    ):
        load_local_batch(
            run_id=RUN_ID,
            partitioned_data_dir=(
                project_root
                / "data"
                / "s3_partitioned"
            ),
        )


def test_pipeline_cli_maps_incremental_partition_source():
    args = pipeline.parse_cli_args(
        [
            "partition",
            "--run-id",
            RUN_ID,
            "--source",
            "incremental",
        ]
    )

    command, _ = pipeline.build_command(args)

    assert command[-2:] == [
        "--source",
        "incremental",
    ]


def test_pipeline_cli_preserves_validated_partition_default():
    args = pipeline.parse_cli_args(
        [
            "partition",
            "--run-id",
            RUN_ID,
        ]
    )

    command, _ = pipeline.build_command(args)

    assert "--source" not in command
