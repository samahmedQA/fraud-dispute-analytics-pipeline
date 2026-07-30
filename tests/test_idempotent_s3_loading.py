from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from scripts.upload_partitioned_to_s3 import (
    EXPECTED_DATASETS,
    UploadConflictError,
    build_run_prefix,
    load_local_batch,
    upload_run,
)


RUN_ID = "20260730T040325Z_cd5bcec2"
BUCKET = "unit-test-bucket"


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}
        self.put_order: list[str] = []
        self.deleted_keys: list[str] = []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        contents = [
            {
                "Key": key,
                "Size": len(value["Body"]),
            }
            for key, value in sorted(self.objects.items())
            if key.startswith(Prefix)
        ]
        return {
            "Contents": contents,
            "IsTruncated": False,
            "KeyCount": len(contents),
        }

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError(
                {
                    "Error": {
                        "Code": "NoSuchKey",
                        "Message": "Not found",
                    }
                },
                "GetObject",
            )

        return {
            "Body": io.BytesIO(self.objects[Key]["Body"]),
        }

    def put_object(
        self,
        Bucket,
        Key,
        Body,
        ContentType=None,
        Metadata=None,
    ):
        if hasattr(Body, "read"):
            Body = Body.read()

        self.objects[Key] = {
            "Body": bytes(Body),
            "Metadata": dict(Metadata or {}),
        }
        self.put_order.append(Key)
        return {"ETag": "fake-etag"}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError(
                {
                    "Error": {
                        "Code": "404",
                        "Message": "Not found",
                    }
                },
                "HeadObject",
            )

        value = self.objects[Key]
        return {
            "ContentLength": len(value["Body"]),
            "Metadata": value["Metadata"],
        }

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            key = item["Key"]
            self.objects.pop(key, None)
            self.deleted_keys.append(key)

        return {"Deleted": Delete["Objects"]}


def create_local_batch(root: Path) -> Path:
    run_directory = root / RUN_ID
    datasets = []
    total_records = 0

    for index, dataset in enumerate(sorted(EXPECTED_DATASETS), start=1):
        relative_file = (
            Path("raw")
            / dataset
            / "year=2026"
            / "month=07"
            / f"{dataset}_2026_07.json"
        )
        file_path = run_directory / relative_file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        records = [
            {"dataset": dataset, "record_number": number}
            for number in range(1, index + 1)
        ]
        file_path.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n",
            encoding="utf-8",
        )
        total_records += len(records)
        datasets.append(
            {
                "dataset": dataset,
                "input_file": f"data/validated/{RUN_ID}/{dataset}.json",
                "date_field": "event_date",
                "records_received": len(records),
                "records_partitioned": len(records),
                "records_missing_date": 0,
                "partition_count": 1,
                "output_files": [str(relative_file)],
            }
        )

    manifest = {
        "run_id": RUN_ID,
        "generated_at_utc": "2026-07-30T04:03:33Z",
        "source_directory": f"data/validated/{RUN_ID}",
        "output_directory": f"data/s3_partitioned/{RUN_ID}",
        "dataset_count": len(datasets),
        "records_received": total_records,
        "records_partitioned": total_records,
        "records_missing_date": 0,
        "datasets": datasets,
    }
    run_directory.mkdir(parents=True, exist_ok=True)
    (run_directory / "partition_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return root


def test_load_local_batch_validates_manifest_and_inventory(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")

    batch = load_local_batch(
        run_id=RUN_ID,
        partitioned_data_dir=partitioned_root,
    )

    assert batch.file_count == 5
    assert batch.records_partitioned == 15
    assert {item.relative_key.split("/", 1)[0] for item in batch.files} == (
        EXPECTED_DATASETS
    )
    assert len(batch.manifest_sha256) == 64
    assert len(batch.inventory_sha256) == 64


def test_dry_run_does_not_contact_s3(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")

    result = upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=False,
        partitioned_data_dir=partitioned_root,
    )

    assert result == "DRY_RUN"


def test_execute_uploads_manifest_and_success_marker_last(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")
    client = FakeS3Client()

    result = upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=True,
        s3_client=client,
        partitioned_data_dir=partitioned_root,
    )

    run_prefix = build_run_prefix("raw", RUN_ID)
    assert result == "UPLOADED"
    assert len(client.objects) == 7
    assert client.put_order[-2] == f"{run_prefix}partition_manifest.json"
    assert client.put_order[-1] == f"{run_prefix}_SUCCESS.json"

    marker = json.loads(
        client.objects[f"{run_prefix}_SUCCESS.json"]["Body"].decode("utf-8")
    )
    assert marker["status"] == "COMPLETE"
    assert marker["data_file_count"] == 5
    assert marker["records_partitioned"] == 15


def test_identical_completed_batch_is_skipped(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")
    client = FakeS3Client()

    upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=True,
        s3_client=client,
        partitioned_data_dir=partitioned_root,
    )
    client.put_order.clear()

    result = upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=True,
        s3_client=client,
        partitioned_data_dir=partitioned_root,
    )

    assert result == "SKIPPED"
    assert client.put_order == []


def test_conflicting_completed_batch_is_blocked(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")
    client = FakeS3Client()

    upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=True,
        s3_client=client,
        partitioned_data_dir=partitioned_root,
    )

    run_prefix = build_run_prefix("raw", RUN_ID)
    marker_key = f"{run_prefix}_SUCCESS.json"
    marker = json.loads(client.objects[marker_key]["Body"].decode("utf-8"))
    marker["inventory_sha256"] = "0" * 64
    client.objects[marker_key]["Body"] = json.dumps(marker).encode("utf-8")

    with pytest.raises(UploadConflictError, match="already contains"):
        upload_run(
            bucket=BUCKET,
            run_id=RUN_ID,
            execute=True,
            s3_client=client,
            partitioned_data_dir=partitioned_root,
        )


def test_partial_prefix_is_blocked_without_success_marker(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")
    client = FakeS3Client()
    run_prefix = build_run_prefix("raw", RUN_ID)
    client.put_object(
        Bucket=BUCKET,
        Key=f"{run_prefix}stale.json",
        Body=b"{}",
        Metadata={},
    )

    with pytest.raises(UploadConflictError, match="without a success marker"):
        upload_run(
            bucket=BUCKET,
            run_id=RUN_ID,
            execute=True,
            s3_client=client,
            partitioned_data_dir=partitioned_root,
        )


def test_allow_overwrite_replaces_partial_prefix(tmp_path):
    partitioned_root = create_local_batch(tmp_path / "s3_partitioned")
    client = FakeS3Client()
    run_prefix = build_run_prefix("raw", RUN_ID)
    stale_key = f"{run_prefix}stale.json"
    client.put_object(
        Bucket=BUCKET,
        Key=stale_key,
        Body=b"{}",
        Metadata={},
    )
    client.put_order.clear()

    result = upload_run(
        bucket=BUCKET,
        run_id=RUN_ID,
        execute=True,
        allow_overwrite=True,
        s3_client=client,
        partitioned_data_dir=partitioned_root,
    )

    assert result == "UPLOADED"
    assert stale_key in client.deleted_keys
    assert stale_key not in client.objects
    assert client.put_order[-1] == f"{run_prefix}_SUCCESS.json"
