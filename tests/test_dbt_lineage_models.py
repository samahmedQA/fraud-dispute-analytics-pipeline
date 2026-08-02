from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_ROOT = (
    PROJECT_ROOT
    / "dbt"
    / "fraud_dispute_dbt"
    / "models"
)


def read_model(relative_path: str) -> str:
    return (
        MODEL_ROOT
        / relative_path
    ).read_text(encoding="utf-8").lower()


@pytest.mark.parametrize(
    "model_file",
    [
        "bronze/br_customers.sql",
        "bronze/br_transactions.sql",
        "bronze/br_fraud_signals.sql",
        "bronze/br_disputes.sql",
        "bronze/br_chargeback_outcomes.sql",
    ],
)
def test_bronze_models_preserve_raw_lineage(
    model_file: str,
) -> None:
    sql = read_model(model_file)

    for column in (
        "pipeline_run_id",
        "source_file",
        "source_row_number",
        "loaded_at",
    ):
        assert column in sql


def test_transaction_silver_preserves_joined_lineage(
) -> None:
    sql = read_model(
        "silver/silver_transactions_enriched.sql"
    )

    assert "t.pipeline_run_id" in sql
    assert "transaction_source_file" in sql
    assert "customer_source_file" in sql
    assert "fraud_signal_source_file" in sql
    assert (
        "t.pipeline_run_id = c.pipeline_run_id"
        in sql
    )
    assert (
        "t.pipeline_run_id = f.pipeline_run_id"
        in sql
    )


def test_dispute_silver_preserves_joined_lineage(
) -> None:
    sql = read_model(
        "silver/silver_dispute_outcomes.sql"
    )

    assert "d.pipeline_run_id" in sql
    assert "dispute_source_file" in sql
    assert "transaction_source_file" in sql
    assert "chargeback_source_file" in sql
    assert (
        "d.pipeline_run_id = t.pipeline_run_id"
        in sql
    )
    assert (
        "d.pipeline_run_id = c.pipeline_run_id"
        in sql
    )


@pytest.mark.parametrize(
    ("model_file", "group_by"),
    [
        (
            "gold/gold_daily_fraud_kpis.sql",
            "group by 1, 2, 3",
        ),
        (
            "gold/gold_daily_dispute_kpis.sql",
            "group by 1, 2, 3",
        ),
        (
            "gold/gold_fraud_summary_by_network.sql",
            "group by 1, 2",
        ),
        (
            "gold/gold_dispute_chargeback_summary_by_network.sql",
            "group by 1, 2",
        ),
    ],
)
def test_gold_models_preserve_batch_grain(
    model_file: str,
    group_by: str,
) -> None:
    sql = read_model(model_file)

    assert "pipeline_run_id" in sql
    assert group_by in sql


def test_batch_metadata_covers_all_datasets(
) -> None:
    sql = read_model(
        "gold/gold_pipeline_batch_metadata.sql"
    )

    for model_name in (
        "br_customers",
        "br_transactions",
        "br_fraud_signals",
        "br_disputes",
        "br_chargeback_outcomes",
    ):
        assert f"ref('{model_name}')" in sql


def test_dbt_lineage_tests_exist() -> None:
    tests_dir = (
        PROJECT_ROOT
        / "dbt"
        / "fraud_dispute_dbt"
        / "tests"
    )

    expected_tests = (
        "assert_bronze_lineage_not_null.sql",
        "assert_silver_lineage_not_null.sql",
        "assert_gold_batch_lineage_not_null.sql",
    )

    for test_name in expected_tests:
        assert (tests_dir / test_name).is_file()
