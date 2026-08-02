from __future__ import annotations

from typing import Any

import pytest

from scripts.validate_data_contracts import (
    DATASETS,
    REFERENCE_RULES,
    register_valid_parent_keys,
    resolve_dataset_dependencies,
    select_valid_records,
    validate_referential_integrity,
)


def test_dataset_order_is_dependency_safe(
) -> None:
    assert tuple(DATASETS) == (
        "customers",
        "transactions",
        "fraud_signals",
        "disputes",
        "chargeback_outcomes",
    )


def test_relationship_rules_are_complete(
) -> None:
    actual_rules = {
        (
            dataset,
            tuple(rule["child_fields"]),
            rule["parent_dataset"],
            tuple(rule["parent_fields"]),
        )
        for dataset, rules
        in REFERENCE_RULES.items()
        for rule in rules
    }

    assert actual_rules == {
        (
            "transactions",
            ("customer_id",),
            "customers",
            ("customer_id",),
        ),
        (
            "transactions",
            (
                "customer_id",
                "account_id",
            ),
            "customers",
            (
                "customer_id",
                "account_id",
            ),
        ),
        (
            "fraud_signals",
            ("transaction_id",),
            "transactions",
            ("transaction_id",),
        ),
        (
            "disputes",
            ("transaction_id",),
            "transactions",
            ("transaction_id",),
        ),
        (
            "chargeback_outcomes",
            ("dispute_id",),
            "disputes",
            ("dispute_id",),
        ),
    }


def test_dependency_resolution_includes_parents(
) -> None:
    assert resolve_dataset_dependencies(
        "chargeback_outcomes"
    ) == (
        "customers",
        "transactions",
        "disputes",
        "chargeback_outcomes",
    )


def test_invalid_parent_cannot_satisfy_child(
) -> None:
    parent_records: list[
        tuple[int, dict[str, Any]]
    ] = [
        (
            1,
            {
                "customer_id":
                    "CUST_000001",
                "account_id":
                    "ACCT_000001",
            },
        ),
        (
            2,
            {
                "customer_id":
                    "CUST_000002",
                "account_id":
                    "ACCT_000002",
            },
        ),
    ]

    # Simulate the second parent record
    # failing schema or duplicate validation.
    valid_records = select_valid_records(
        records=parent_records,
        invalid_record_indexes={1},
    )

    registry = {}

    register_valid_parent_keys(
        dataset_name="customers",
        valid_records=valid_records,
        registry=registry,
    )

    child_records = [
        (
            1,
            {
                "transaction_id":
                    "TXN_00000001",
                "customer_id":
                    "CUST_000002",
                "account_id":
                    "ACCT_000002",
            },
        ),
    ]

    failures, invalid_indexes = (
        validate_referential_integrity(
            dataset_name="transactions",
            records=child_records,
            primary_key="transaction_id",
            valid_parent_keys=registry,
        )
    )

    assert invalid_indexes == {0}
    assert failures
    assert all(
        failure["rule"]
        == "referential_integrity"
        for failure in failures
    )


def test_composite_relationship_rejects_wrong_account(
) -> None:
    registry = {}

    register_valid_parent_keys(
        dataset_name="customers",
        valid_records=[
            {
                "customer_id":
                    "CUST_000001",
                "account_id":
                    "ACCT_000001",
            },
            {
                "customer_id":
                    "CUST_000002",
                "account_id":
                    "ACCT_000002",
            },
        ],
        registry=registry,
    )

    child_records = [
        (
            1,
            {
                "transaction_id":
                    "TXN_00000001",
                "customer_id":
                    "CUST_000001",
                "account_id":
                    "ACCT_000002",
            },
        ),
    ]

    failures, invalid_indexes = (
        validate_referential_integrity(
            dataset_name="transactions",
            records=child_records,
            primary_key="transaction_id",
            valid_parent_keys=registry,
        )
    )

    assert invalid_indexes == {0}

    composite_failures = [
        failure
        for failure in failures
        if failure["field"]
        == "customer_id, account_id"
    ]

    assert len(composite_failures) == 1


def test_valid_relationships_are_accepted(
) -> None:
    registry = {}

    register_valid_parent_keys(
        dataset_name="customers",
        valid_records=[
            {
                "customer_id":
                    "CUST_000001",
                "account_id":
                    "ACCT_000001",
            },
        ],
        registry=registry,
    )

    child_records = [
        (
            1,
            {
                "transaction_id":
                    "TXN_00000001",
                "customer_id":
                    "CUST_000001",
                "account_id":
                    "ACCT_000001",
            },
        ),
    ]

    failures, invalid_indexes = (
        validate_referential_integrity(
            dataset_name="transactions",
            records=child_records,
            primary_key="transaction_id",
            valid_parent_keys=registry,
        )
    )

    assert failures == []
    assert invalid_indexes == set()


def test_missing_parent_registry_fails_fast(
) -> None:
    with pytest.raises(
        RuntimeError,
        match=(
            "Parent dataset has not been "
            "validated"
        ),
    ):
        validate_referential_integrity(
            dataset_name="disputes",
            records=[
                (
                    1,
                    {
                        "dispute_id":
                            "DISP_0000001",
                        "transaction_id":
                            "TXN_00000001",
                    },
                ),
            ],
            primary_key="dispute_id",
            valid_parent_keys={},
        )
