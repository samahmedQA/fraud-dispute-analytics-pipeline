import argparse
import os
import re
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def split_sql_statements(sql_text):
    statements = []

    for statement in sql_text.split(";"):
        cleaned_statement = statement.strip()

        if cleaned_statement:
            statements.append(cleaned_statement + ";")

    return statements


def get_snowflake_connection():
    load_dotenv(PROJECT_ROOT / ".env")

    required_env_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]

    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing_vars)
        )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )


def run_sql_file(sql_file_path, dry_run):
    sql_path = PROJECT_ROOT / sql_file_path

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)

    print(f"SQL file: {sql_path}")
    print(f"Statements found: {len(statements)}")

    if dry_run:
        print("\nDRY RUN mode. No SQL will be executed.\n")

        for index, statement in enumerate(statements, start=1):
            first_line = statement.splitlines()[0]
            print(f"{index}. {first_line}")

        return

    print("\nEXECUTE mode. SQL will be executed in Snowflake.\n")

    connection = get_snowflake_connection()

    try:
        cursor = connection.cursor()

        for index, statement in enumerate(statements, start=1):
            preview = statement.splitlines()[0]
            print(f"Executing statement {index}/{len(statements)}: {preview}")
            cursor.execute(statement)

        print("\nSQL file executed successfully.")

    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run a local SQL file against Snowflake."
    )

    parser.add_argument(
        "--sql-file",
        required=True,
        help="Path to SQL file relative to the project root.",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute SQL. Without this flag, the script runs as dry run.",
    )

    args = parser.parse_args()

    run_sql_file(
        sql_file_path=args.sql_file,
        dry_run=not args.execute,
    )


if __name__ == "__main__":
    main()
