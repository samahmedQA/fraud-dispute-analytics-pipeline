from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.validate_data_contracts import (
    is_pipeline_date,
    is_pipeline_date_time,
    validate_schema,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-01-01", True),
        ("2026-02-28", True),
        ("2028-02-29", True),
        ("2026-02-29", False),
        ("2026-02-30", False),
        ("2026-13-01", False),
        ("2026-00-10", False),
        ("2026-01-00", False),
        ("2026-1-01", False),
    ],
)
def test_pipeline_date_semantics(
    value: str,
    expected: bool,
) -> None:
    assert is_pipeline_date(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "2026-07-21 00:00:00",
            True,
        ),
        (
            "2026-07-21 23:59:59",
            True,
        ),
        (
            "2028-02-29 12:30:45",
            True,
        ),
        (
            "2026-02-30 12:30:45",
            False,
        ),
        (
            "2026-07-21 24:00:00",
            False,
        ),
        (
            "2026-07-21 12:60:00",
            False,
        ),
        (
            "2026-07-21 12:30:60",
            False,
        ),
        (
            "2026-07-21T12:30:00",
            False,
        ),
    ],
)
def test_pipeline_date_time_semantics(
    value: str,
    expected: bool,
) -> None:
    assert (
        is_pipeline_date_time(value)
        is expected
    )


def test_validate_schema_rejects_impossible_timestamp(
) -> None:
    schema: dict[str, Any] = {
        "type": "object",
        "required": [
            "record_id",
            "event_timestamp",
        ],
        "properties": {
            "record_id": {
                "type": "string",
            },
            "event_timestamp": {
                "type": "string",
                "pattern": (
                    "^[0-9]{4}-[0-9]{2}-[0-9]{2} "
                    "[0-9]{2}:[0-9]{2}:[0-9]{2}$"
                ),
                "format": "pipeline-date-time",
            },
        },
        "additionalProperties": False,
    }

    records = [
        (
            1,
            {
                "record_id": "REC_001",
                "event_timestamp":
                    "2026-99-99 25:61:61",
            },
        ),
    ]

    failures, invalid_indexes = validate_schema(
        dataset_name="test_events",
        records=records,
        schema=schema,
        primary_key="record_id",
    )

    assert invalid_indexes == {0}
    assert len(failures) == 1
    assert failures[0]["field"] == (
        "event_timestamp"
    )
    assert failures[0]["rule"] == "format"
    assert failures[0]["severity"] == (
        "hard_fail"
    )


@pytest.mark.parametrize(
    (
        "schema_file",
        "field_name",
        "expected_format",
    ),
    [
        (
            "customers.schema.json",
            "created_at",
            "pipeline-date",
        ),
        (
            "transactions.schema.json",
            "transaction_timestamp",
            "pipeline-date-time",
        ),
        (
            "fraud_signals.schema.json",
            "score_timestamp",
            "pipeline-date-time",
        ),
        (
            "disputes.schema.json",
            "opened_date",
            "pipeline-date",
        ),
        (
            "chargeback_outcomes.schema.json",
            "resolved_date",
            "pipeline-date",
        ),
    ],
)
def test_contract_declares_semantic_format(
    schema_file: str,
    field_name: str,
    expected_format: str,
) -> None:
    schema_path = (
        PROJECT_ROOT
        / "contracts"
        / "v1"
        / schema_file
    )

    schema = json.loads(
        schema_path.read_text(
            encoding="utf-8"
        )
    )

    actual_format = schema[
        "properties"
    ][field_name]["format"]

    assert actual_format == expected_format
