import os
from decimal import Decimal

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

st.set_page_config(
    page_title="Fraud & Dispute Analytics Dashboard",
    layout="wide"
)

st.title("Fraud & Dispute Analytics Dashboard")
st.caption("Live fraud, dispute, chargeback, and pipeline-health analytics from Snowflake Gold and Monitoring layers")


REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]


def validate_env_vars():
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]

    if missing:
        st.error(
            "Missing Snowflake environment variables: "
            + ", ".join(missing)
            + ". Make sure your .env file exists locally and is not committed."
        )
        st.stop()


def get_snowflake_connection():
    validate_env_vars()

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        token=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()
        conn.close()


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()

    for column in cleaned_df.columns:
        if cleaned_df[column].map(lambda value: isinstance(value, Decimal)).any():
            cleaned_df[column] = cleaned_df[column].astype(float)

    return cleaned_df


def get_first_existing_column(df: pd.DataFrame, possible_columns: list[str]):
    for column in possible_columns:
        if column in df.columns:
            return column
    return None


def get_table_name(schema_name: str, table_name: str) -> str:
    database_name = os.getenv("SNOWFLAKE_DATABASE")
    return f"{database_name}.{schema_name}.{table_name}"


try:
    fraud_summary = convert_numeric_columns(run_query(f"""
        SELECT *
        FROM {get_table_name("MARTS", "GOLD_FRAUD_SUMMARY_BY_NETWORK")}
        ORDER BY CARD_NETWORK
    """))

    dispute_summary = convert_numeric_columns(run_query(f"""
        SELECT *
        FROM {get_table_name("MARTS", "GOLD_DISPUTE_CHARGEBACK_SUMMARY_BY_NETWORK")}
        ORDER BY CARD_NETWORK
    """))

    daily_fraud = convert_numeric_columns(run_query(f"""
        SELECT *
        FROM {get_table_name("MARTS", "GOLD_DAILY_FRAUD_KPIS")}
        ORDER BY 1, 2
    """))

    daily_disputes = convert_numeric_columns(run_query(f"""
        SELECT *
        FROM {get_table_name("MARTS", "GOLD_DAILY_DISPUTE_KPIS")}
        ORDER BY 1, 2
    """))

    row_counts = convert_numeric_columns(run_query(f"""
        SELECT *
        FROM {get_table_name("MONITORING", "MONITORING_PIPELINE_ROW_COUNTS")}
        ORDER BY 1, 2
    """))

except Exception as error:
    st.error("Dashboard failed while querying Snowflake.")
    st.exception(error)
    st.stop()


st.header("Executive Summary")

col1, col2, col3, col4 = st.columns(4)

total_transactions = int(fraud_summary["TOTAL_TRANSACTIONS"].sum())
high_risk_transactions = int(fraud_summary["HIGH_RISK_TRANSACTIONS"].sum())
total_disputes = int(dispute_summary["TOTAL_DISPUTES"].sum())
total_chargebacks = int(dispute_summary["TOTAL_CHARGEBACKS"].sum())

high_risk_rate = (
    high_risk_transactions / total_transactions * 100
    if total_transactions
    else 0.0
)

chargeback_rate = (
    total_chargebacks / total_disputes * 100
    if total_disputes
    else 0.0
)

col1.metric("Total Transactions", f"{total_transactions:,}")
col2.metric("High-Risk Rate", f"{high_risk_rate:.1f}%")
col3.metric("Total Disputes", f"{total_disputes:,}")
col4.metric("Chargeback Rate", f"{chargeback_rate:.1f}%")

run_ids = sorted(
    fraud_summary["PIPELINE_RUN_ID"].dropna().astype(str).unique(),
    reverse=True,
)

with st.expander("Technical Run Details"):
    if run_ids:
        st.write(f"Latest pipeline run: `{run_ids[0]}`")
    st.write("Source: Snowflake Gold and Monitoring layers")


st.header("Fraud Risk by Card Network")

high_risk_chart = fraud_summary.set_index("CARD_NETWORK")[["HIGH_RISK_RATE_PCT"]]
st.bar_chart(high_risk_chart)

with st.expander("View fraud summary data"):
    st.dataframe(
        fraud_summary.drop(columns=["PIPELINE_RUN_ID"], errors="ignore"),
        use_container_width=True,
    )


st.header("Dispute & Chargeback Outcomes")

dispute_chart = dispute_summary.set_index("CARD_NETWORK")[[
    "TOTAL_DISPUTES",
    "TOTAL_CHARGEBACKS",
]]
st.bar_chart(dispute_chart, stack=False)

with st.expander("View dispute summary data"):
    st.dataframe(
        dispute_summary.drop(columns=["PIPELINE_RUN_ID"], errors="ignore"),
        use_container_width=True,
    )


st.header("Daily Trends")

fraud_tab, dispute_tab = st.tabs(["Fraud Activity", "Dispute Activity"])

with fraud_tab:
    fraud_trend = (
        daily_fraud
        .groupby("TRANSACTION_DATE", as_index=False)[
            ["TOTAL_TRANSACTIONS", "HIGH_RISK_TRANSACTIONS"]
        ]
        .sum()
        .sort_values("TRANSACTION_DATE")
        .set_index("TRANSACTION_DATE")
        .rename(columns={
            "TOTAL_TRANSACTIONS": "Total Transactions",
            "HIGH_RISK_TRANSACTIONS": "High-Risk Transactions",
        })
    )

    st.line_chart(fraud_trend)

    with st.expander("View daily fraud data"):
        st.dataframe(
            daily_fraud.drop(columns=["PIPELINE_RUN_ID"], errors="ignore"),
            use_container_width=True,
        )

with dispute_tab:
    dispute_trend = (
        daily_disputes
        .groupby("DISPUTE_OPENED_DATE", as_index=False)[
            ["TOTAL_DISPUTES", "TOTAL_CHARGEBACKS"]
        ]
        .sum()
        .sort_values("DISPUTE_OPENED_DATE")
        .set_index("DISPUTE_OPENED_DATE")
        .rename(columns={
            "TOTAL_DISPUTES": "Total Disputes",
            "TOTAL_CHARGEBACKS": "Total Chargebacks",
        })
    )

    st.line_chart(dispute_trend)

    with st.expander("View daily dispute data"):
        st.dataframe(
            daily_disputes.drop(columns=["PIPELINE_RUN_ID"], errors="ignore"),
            use_container_width=True,
        )


st.header("Pipeline Health")

monitored_objects = row_counts["OBJECT_NAME"].nunique()
monitored_layers = row_counts["LAYER"].nunique()
latest_check = pd.to_datetime(row_counts["CHECKED_AT"]).max()

health1, health2, health3 = st.columns(3)
health1.metric("Monitored Objects", f"{monitored_objects:,}")
health2.metric("Pipeline Layers", f"{monitored_layers:,}")
health3.metric("Last Checked", latest_check.strftime("%b %d, %Y %H:%M"))

st.subheader("Rows by Pipeline Layer")

layer_counts = (
    row_counts
    .groupby("LAYER", as_index=False)["ROW_COUNT"]
    .sum()
    .set_index("LAYER")
    .rename(columns={"ROW_COUNT": "Rows"})
)

st.bar_chart(layer_counts)

with st.expander("View pipeline monitoring data"):
    st.dataframe(row_counts, use_container_width=True)

st.caption("Synthetic portfolio data only.")
