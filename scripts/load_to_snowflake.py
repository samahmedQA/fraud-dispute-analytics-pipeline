import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

FILES_TO_TABLES = {
    "customers.json": "RAW_CUSTOMERS",
    "transactions.json": "RAW_TRANSACTIONS",
    "fraud_signals.json": "RAW_FRAUD_SIGNALS",
    "disputes.json": "RAW_DISPUTES",
    "chargeback_outcomes.json": "RAW_CHARGEBACK_OUTCOMES",
}


def main():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DISPUTE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )

    cur = conn.cursor()

    try:
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute("USE DATABASE FRAUD_DISPUTE_DB")
        cur.execute("USE SCHEMA RAW")

        for file_name, table_name in FILES_TO_TABLES.items():
            file_path = DATA_DIR / file_name

            if not file_path.exists():
                raise FileNotFoundError(f"Missing file: {file_path}")

            file_uri = "file://" + str(file_path).replace("\\", "/")

            print(f"Loading {file_name} into {table_name}...")

            cur.execute(f"TRUNCATE TABLE {table_name}")

            cur.execute(
                f"""
                PUT {file_uri}
                @RAW_JSON_STAGE
                AUTO_COMPRESS = FALSE
                OVERWRITE = TRUE
                """
            )

            cur.execute(
                f"""
                COPY INTO {table_name} (raw_record)
                FROM (
                    SELECT $1
                    FROM @RAW_JSON_STAGE/{file_name}
                )
                FILE_FORMAT = (FORMAT_NAME = JSON_LINES_FORMAT)
                ON_ERROR = 'ABORT_STATEMENT'
                """
            )

            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cur.fetchone()[0]
            print(f"{table_name}: {row_count} rows loaded")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()