from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARTITIONED_DATA_DIR = PROJECT_ROOT / "data" / "s3_partitioned"

RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z_[0-9a-f]{8}$")
EXPECTED_DATASETS = {
    "chargeback_outcomes",
    "customers",
    "disputes",
    "fraud_signals",
    "transactions",
}


class UploadConflictError(RuntimeError):
    """Raised when an S3 run prefix contains conflicting or incomplete data."""


@dataclass(frozen=True)
class LocalFile:
    relative_key: str
    path: Path
    size: int
    sha256: str


@dataclass(frozen=True)
class LocalBatch:
    run_id: str
    run_directory: Path
    raw_directory: Path
    manifest_path: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    inventory_sha256: str
    records_partitioned: int
    files: tuple[LocalFile, ...]

    @property
    def file_count(self) -> int:
        return len(self.files)


def validate_run_id(run_id: str) -> str:
    """Validate a pipeline run ID such as 20260729T213000Z_a1b2c3d4."""

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise argparse.ArgumentTypeError(
            "run ID must match YYYYMMDDTHHMMSSZ_aaaaaaaa"
        )

    return run_id


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def normalize_manifest_path(value: str) -> str:
    return value.replace("\\", "/").lstrip("/")


def calculate_inventory_sha256(files: list[LocalFile]) -> str:
    inventory_lines = [
        f"{item.relative_key}|{item.size}|{item.sha256}"
        for item in sorted(files, key=lambda item: item.relative_key)
    ]
    return sha256_bytes("\n".join(inventory_lines).encode("utf-8"))


def load_local_batch(
    run_id: str,
    partitioned_data_dir: Path = PARTITIONED_DATA_DIR,
) -> LocalBatch:
    run_directory = partitioned_data_dir / run_id
    raw_directory = run_directory / "raw"
    manifest_path = run_directory / "partition_manifest.json"

    if not raw_directory.is_dir():
        raise FileNotFoundError(
            "Local partitioned raw directory does not exist: "
            f"{raw_directory}. Run scripts/partition_data_for_s3.py "
            "with the same --run-id first."
        )

    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Partition manifest does not exist: {manifest_path}"
        )

    manifest_bytes = manifest_path.read_bytes()

    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Partition manifest is not valid JSON: {manifest_path}"
        ) from error

    if manifest.get("run_id") != run_id:
        raise ValueError(
            "Manifest run_id does not match requested run ID: "
            f"{manifest.get('run_id')!r} != {run_id!r}"
        )

    datasets = manifest.get("datasets")

    if not isinstance(datasets, list):
        raise ValueError("Manifest datasets must be a list.")

    dataset_names = {
        dataset.get("dataset")
        for dataset in datasets
        if isinstance(dataset, dict)
    }

    if dataset_names != EXPECTED_DATASETS:
        missing = sorted(EXPECTED_DATASETS - dataset_names)
        unexpected = sorted(dataset_names - EXPECTED_DATASETS)
        raise ValueError(
            "Manifest dataset set is invalid. "
            f"Missing: {missing or 'none'}; "
            f"Unexpected: {unexpected or 'none'}."
        )

    if manifest.get("dataset_count") != len(EXPECTED_DATASETS):
        raise ValueError(
            "Manifest dataset_count does not match the required dataset set."
        )

    expected_relative_paths: list[str] = []
    dataset_record_total = 0

    for dataset in datasets:
        dataset_name = dataset["dataset"]
        output_files = dataset.get("output_files")

        if not isinstance(output_files, list) or not output_files:
            raise ValueError(
                f"Manifest dataset {dataset_name} has no output files."
            )

        if dataset.get("partition_count") != len(output_files):
            raise ValueError(
                f"Manifest partition_count mismatch for {dataset_name}."
            )

        if dataset.get("records_missing_date") != 0:
            raise ValueError(
                f"Manifest reports records missing dates for {dataset_name}."
            )

        records_partitioned = dataset.get("records_partitioned")

        if not isinstance(records_partitioned, int) or records_partitioned < 0:
            raise ValueError(
                f"Manifest records_partitioned is invalid for {dataset_name}."
            )

        dataset_record_total += records_partitioned

        for output_file in output_files:
            relative_path = normalize_manifest_path(str(output_file))
            expected_prefix = f"raw/{dataset_name}/"

            if not relative_path.startswith(expected_prefix):
                raise ValueError(
                    f"Manifest output file is outside {expected_prefix}: "
                    f"{relative_path}"
                )

            expected_relative_paths.append(relative_path.removeprefix("raw/"))

    if len(expected_relative_paths) != len(set(expected_relative_paths)):
        raise ValueError("Manifest contains duplicate output file paths.")

    manifest_record_total = manifest.get("records_partitioned")

    if manifest_record_total != dataset_record_total:
        raise ValueError(
            "Manifest records_partitioned does not equal the dataset total."
        )

    if manifest.get("records_missing_date") != 0:
        raise ValueError("Manifest reports records missing partition dates.")

    files: list[LocalFile] = []

    for relative_key in sorted(expected_relative_paths):
        file_path = raw_directory / Path(relative_key)

        if not file_path.is_file():
            raise FileNotFoundError(
                f"Manifest output file does not exist: {file_path}"
            )

        files.append(
            LocalFile(
                relative_key=relative_key,
                path=file_path,
                size=file_path.stat().st_size,
                sha256=sha256_file(file_path),
            )
        )

    actual_json_files = {
        path.relative_to(raw_directory).as_posix()
        for path in raw_directory.rglob("*.json")
        if path.is_file()
    }
    expected_json_files = {item.relative_key for item in files}

    if actual_json_files != expected_json_files:
        extra = sorted(actual_json_files - expected_json_files)
        missing = sorted(expected_json_files - actual_json_files)
        raise ValueError(
            "Local partitioned files do not match the manifest. "
            f"Missing: {missing or 'none'}; Extra: {extra or 'none'}."
        )

    return LocalBatch(
        run_id=run_id,
        run_directory=run_directory,
        raw_directory=raw_directory,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=sha256_bytes(manifest_bytes),
        inventory_sha256=calculate_inventory_sha256(files),
        records_partitioned=dataset_record_total,
        files=tuple(files),
    )


def build_run_prefix(prefix: str, run_id: str) -> str:
    clean_prefix = prefix.strip("/")
    prefix_part = f"{clean_prefix}/" if clean_prefix else ""
    return f"{prefix_part}run_id={run_id}/"


def list_remote_objects(
    s3_client: Any,
    bucket: str,
    run_prefix: str,
) -> dict[str, int]:
    objects: dict[str, int] = {}
    continuation_token: str | None = None

    while True:
        request: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": run_prefix,
        }

        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**request)

        for item in response.get("Contents", []):
            objects[item["Key"]] = int(item["Size"])

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

        if not continuation_token:
            raise RuntimeError(
                "S3 returned a truncated listing without a continuation token."
            )

    return objects


def get_json_object(
    s3_client: Any,
    bucket: str,
    key: str,
) -> dict[str, Any] | None:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")

        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None

        raise

    body = response["Body"].read()

    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise UploadConflictError(
            f"Remote JSON control file is invalid: s3://{bucket}/{key}"
        ) from error


def delete_run_prefix(
    s3_client: Any,
    bucket: str,
    objects: dict[str, int],
) -> None:
    keys = sorted(objects)

    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )


def marker_matches_batch(marker: dict[str, Any], batch: LocalBatch) -> bool:
    return (
        marker.get("status") == "COMPLETE"
        and marker.get("run_id") == batch.run_id
        and marker.get("manifest_sha256") == batch.manifest_sha256
        and marker.get("inventory_sha256") == batch.inventory_sha256
        and marker.get("data_file_count") == batch.file_count
        and marker.get("records_partitioned") == batch.records_partitioned
    )


def expected_remote_inventory(
    batch: LocalBatch,
    run_prefix: str,
) -> dict[str, LocalFile]:
    return {
        f"{run_prefix}{item.relative_key}": item
        for item in batch.files
    }


def verify_remote_batch(
    s3_client: Any,
    bucket: str,
    run_prefix: str,
    batch: LocalBatch,
    include_success_marker: bool,
) -> None:
    remote_objects = list_remote_objects(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
    )
    expected_data = expected_remote_inventory(batch, run_prefix)
    manifest_key = f"{run_prefix}partition_manifest.json"
    success_key = f"{run_prefix}_SUCCESS.json"
    expected_keys = set(expected_data) | {manifest_key}

    if include_success_marker:
        expected_keys.add(success_key)

    remote_keys = set(remote_objects)

    if remote_keys != expected_keys:
        missing = sorted(expected_keys - remote_keys)
        unexpected = sorted(remote_keys - expected_keys)
        raise UploadConflictError(
            "Remote run prefix does not match the expected inventory. "
            f"Missing: {missing or 'none'}; "
            f"Unexpected: {unexpected or 'none'}."
        )

    for key, local_file in expected_data.items():
        if remote_objects[key] != local_file.size:
            raise UploadConflictError(
                f"Remote file size mismatch: s3://{bucket}/{key}"
            )

        head = s3_client.head_object(Bucket=bucket, Key=key)
        remote_sha256 = head.get("Metadata", {}).get("sha256")

        if remote_sha256 != local_file.sha256:
            raise UploadConflictError(
                f"Remote file checksum mismatch: s3://{bucket}/{key}"
            )

    manifest_head = s3_client.head_object(Bucket=bucket, Key=manifest_key)

    if manifest_head.get("Metadata", {}).get("sha256") != batch.manifest_sha256:
        raise UploadConflictError(
            f"Remote manifest checksum mismatch: s3://{bucket}/{manifest_key}"
        )


def upload_batch_objects(
    s3_client: Any,
    bucket: str,
    run_prefix: str,
    batch: LocalBatch,
) -> None:
    for index, item in enumerate(batch.files, start=1):
        key = f"{run_prefix}{item.relative_key}"
        print(
            f"Uploading file {index}/{batch.file_count}: "
            f"s3://{bucket}/{key}"
        )
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=item.path.read_bytes(),
            ContentType="application/json",
            Metadata={
                "pipeline-run-id": batch.run_id,
                "sha256": item.sha256,
            },
        )

    manifest_bytes = batch.manifest_path.read_bytes()
    manifest_key = f"{run_prefix}partition_manifest.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
        Metadata={
            "pipeline-run-id": batch.run_id,
            "sha256": batch.manifest_sha256,
        },
    )


def write_success_marker(
    s3_client: Any,
    bucket: str,
    run_prefix: str,
    batch: LocalBatch,
) -> None:
    marker = {
        "status": "COMPLETE",
        "run_id": batch.run_id,
        "completed_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "manifest_sha256": batch.manifest_sha256,
        "inventory_sha256": batch.inventory_sha256,
        "data_file_count": batch.file_count,
        "records_partitioned": batch.records_partitioned,
    }
    marker_bytes = json.dumps(
        marker,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    success_key = f"{run_prefix}_SUCCESS.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=success_key,
        Body=marker_bytes,
        ContentType="application/json",
        Metadata={
            "pipeline-run-id": batch.run_id,
            "sha256": sha256_bytes(marker_bytes),
        },
    )


def upload_run(
    bucket: str,
    run_id: str,
    prefix: str = "raw",
    execute: bool = False,
    allow_overwrite: bool = False,
    s3_client: Any | None = None,
    partitioned_data_dir: Path = PARTITIONED_DATA_DIR,
) -> str:
    batch = load_local_batch(
        run_id=run_id,
        partitioned_data_dir=partitioned_data_dir,
    )
    run_prefix = build_run_prefix(prefix=prefix, run_id=run_id)
    s3_uri = f"s3://{bucket}/{run_prefix}"

    print(f"Pipeline run ID: {run_id}")
    print(f"Manifest: {batch.manifest_path}")
    print(f"Files validated: {batch.file_count}")
    print(f"Records validated: {batch.records_partitioned}")
    print(f"S3 target: {s3_uri}")

    if not execute:
        print("Running in DRY RUN mode. No files will be uploaded.")
        print("Remote conflict and completion-marker checks were not executed.")
        return "DRY_RUN"

    if s3_client is None:
        s3_client = boto3.client("s3")

    remote_objects = list_remote_objects(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
    )
    success_key = f"{run_prefix}_SUCCESS.json"
    marker = get_json_object(
        s3_client=s3_client,
        bucket=bucket,
        key=success_key,
    )

    if marker is not None and marker_matches_batch(marker, batch):
        try:
            verify_remote_batch(
                s3_client=s3_client,
                bucket=bucket,
                run_prefix=run_prefix,
                batch=batch,
                include_success_marker=True,
            )
        except UploadConflictError:
            if not allow_overwrite:
                raise
        else:
            print("Identical completed batch already exists in S3.")
            print("Upload skipped safely.")
            return "SKIPPED"

    if remote_objects and not allow_overwrite:
        state = (
            "a conflicting completed batch"
            if marker is not None
            else "an incomplete or legacy batch without a success marker"
        )
        raise UploadConflictError(
            f"S3 prefix already contains {state}: {s3_uri}. "
            "Use --allow-overwrite only when replacement is intentional."
        )

    if remote_objects and allow_overwrite:
        print(
            "Existing run prefix will be replaced because "
            "--allow-overwrite was set."
        )
        delete_run_prefix(
            s3_client=s3_client,
            bucket=bucket,
            objects=remote_objects,
        )

    print("Running in EXECUTE mode. Files will be uploaded to S3.")
    upload_batch_objects(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
        batch=batch,
    )
    verify_remote_batch(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
        batch=batch,
        include_success_marker=False,
    )
    write_success_marker(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
        batch=batch,
    )
    verify_remote_batch(
        s3_client=s3_client,
        bucket=bucket,
        run_prefix=run_prefix,
        batch=batch,
        include_success_marker=True,
    )

    print("S3 upload completed and verified.")
    print("Completion marker written last.")
    return "UPLOADED"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload one validated pipeline run to an idempotent S3 raw zone."
        )
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Target S3 bucket name.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        type=validate_run_id,
        help="Pipeline run ID associated with the partitioned data.",
    )
    parser.add_argument(
        "--prefix",
        default="raw",
        help="Target S3 prefix. Default: raw",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually upload files. Without this flag, run a local dry run.",
    )
    parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        help=(
            "Delete and replace an existing conflicting run prefix. "
            "Use only when replacement is intentional."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        upload_run(
            bucket=args.bucket,
            run_id=args.run_id,
            prefix=args.prefix,
            execute=args.execute,
            allow_overwrite=args.allow_overwrite,
        )
    except (
        FileNotFoundError,
        ValueError,
        UploadConflictError,
        ClientError,
    ) as error:
        print(f"S3 upload failed: {error}")
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
