import os
from decimal import Decimal

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

st.set_page_config(
    page_title="Fraud & Dispute Analytics Pipeline",
    layout="wide"
)

st.title("Fraud & Dispute Analytics Pipeline")
st.caption("AWS S3 -> Snowflake -> dbt -> Gold Marts -> Streamlit Dashboard")


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
        password=os.getenv("SNOWFLAKE_PASSWORD"),
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


try:
    fraud_summary = convert_numeric_columns(run_query("""
        SELECT *
        FROM FRAUD_DISPUTE_DB.MARTS.GOLD_FRAUD_SUMMARY_BY_NETWORK
        ORDER BY CARD_NETWORK
    """))

    dispute_summary = convert_numeric_columns(run_query("""
        SELECT *
        FROM FRAUD_DISPUTE_DB.MARTS.GOLD_DISPUTE_CHARGEBACK_SUMMARY_BY_NETWORK
        ORDER BY CARD_NETWORK
    """))

    daily_fraud = convert_numeric_columns(run_query("""
        SELECT *
        FROM FRAUD_DISPUTE_DB.MARTS.GOLD_DAILY_FRAUD_KPIS
        ORDER BY 1, 2
    """))

    daily_disputes = convert_numeric_columns(run_query("""
        SELECT *
        FROM FRAUD_DISPUTE_DB.MARTS.GOLD_DAILY_DISPUTE_KPIS
        ORDER BY 1, 2
    """))

    row_counts = convert_numeric_columns(run_query("""
        SELECT *
        FROM FRAUD_DISPUTE_DB.MONITORING.MONITORING_PIPELINE_ROW_COUNTS
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

col1.metric("Total Transactions", f"{total_transactions:,}")
col2.metric("High-Risk Transactions", f"{high_risk_transactions:,}")
col3.metric("Total Disputes", f"{total_disputes:,}")
col4.metric("Total Chargebacks", f"{total_chargebacks:,}")


st.header("Fraud Risk by Card Network")

left, right = st.columns(2)

with left:
    st.subheader("Fraud Summary")
    st.dataframe(fraud_summary, use_container_width=True)

with right:
    st.subheader("High-Risk Rate %")
    high_risk_chart = fraud_summary.set_index("CARD_NETWORK")[["HIGH_RISK_RATE_PCT"]]
    st.bar_chart(high_risk_chart)


st.header("Dispute & Chargeback Outcomes")

left, right = st.columns(2)

with left:
    st.subheader("Dispute Summary")
    st.dataframe(dispute_summary, use_container_width=True)

with right:
    st.subheader("Disputes vs Chargebacks")
    dispute_chart = dispute_summary.set_index("CARD_NETWORK")[[
        "TOTAL_DISPUTES",
        "TOTAL_CHARGEBACKS"
    ]]
    st.bar_chart(dispute_chart)


st.header("Daily KPI Tables")

tab1, tab2 = st.tabs(["Daily Fraud KPIs", "Daily Dispute KPIs"])

with tab1:
    st.dataframe(daily_fraud, use_container_width=True)

with tab2:
    st.dataframe(daily_disputes, use_container_width=True)


st.header("Pipeline Monitoring")

st.subheader("Row Counts")
st.dataframe(row_counts, use_container_width=True)

row_count_column = get_first_existing_column(row_counts, ["ROW_COUNT", "RECORD_COUNT", "COUNT"])
group_column = get_first_existing_column(
    row_counts,
    ["SCHEMA_NAME", "TABLE_SCHEMA", "SCHEMA", "LAYER", "PIPELINE_LAYER", "TABLE_NAME"]
)

if row_count_column and group_column:
    monitoring_chart = row_counts.groupby(group_column)[row_count_column].sum()
    st.bar_chart(monitoring_chart)
else:
    st.info("Monitoring chart skipped because the expected row-count columns were not found.")


st.caption("Synthetic data only. No production data, customer data, or company data is used.")
