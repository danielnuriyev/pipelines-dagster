#!/usr/bin/env python3
"""
Populate the SNOWFLAKE.PUBLIC.test_a table with 100 records with different values.
Creates the PUBLIC schema in SNOWFLAKE database if it does not exist.

Note: Requires a running Snowflake emulator or compatible server.

Usage:
    uv run python populate_snowflake.py [--host HOST] [--port PORT] [--user USER] [--password PASSWORD] [--database DATABASE] [--schema SCHEMA] [--warehouse WAREHOUSE]

Example:
    uv run python populate_snowflake.py --host localhost --port 8088 --user snowflake --password snowflake

Prerequisites:
    1. Snowflake emulator or compatible server running
    2. Port forwarding if running in Kubernetes:
       kubectl port-forward svc/snowflake-emulator -n trino 8088:8088 &
"""

import argparse
from datetime import datetime, timedelta

import snowflake.connector


def populate_test_a(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str = "SNOWFLAKE",
    schema: str = "PUBLIC",
    warehouse: str = "COMPUTE_WH"
) -> None:
    """Populate test_a table with 100 records with different values."""
    conn = snowflake.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )
    cursor = conn.cursor()

    target_database = database
    target_schema = schema
    target_table = "test_a"
    target_full_name = f"{target_database}.{target_schema}.{target_table}"

    print(f"Populating {target_full_name} with 100 records...")

    # Check if database exists, if not create it
    cursor.execute(f"SHOW DATABASES LIKE '{target_database}'")
    database_exists = len(cursor.fetchall()) > 0

    if not database_exists:
        print(f"Creating database {target_database}...")
        cursor.execute(f"CREATE DATABASE {target_database}")
        conn.commit()

    # Check if schema exists, if not create it
    cursor.execute(f"SHOW SCHEMAS LIKE '{target_schema}' IN DATABASE {target_database}")
    schema_exists = len(cursor.fetchall()) > 0

    if not schema_exists:
        print(f"Creating schema {target_database}.{target_schema}...")
        cursor.execute(f"CREATE SCHEMA {target_database}.{target_schema}")
        conn.commit()

    # Check if table exists, if not create it
    cursor.execute(f"SHOW TABLES LIKE '{target_table}' IN {target_database}.{target_schema}")
    table_exists = len(cursor.fetchall()) > 0

    if not table_exists:
        print(f"Creating table {target_full_name}...")
        cursor.execute(f"""
            CREATE TABLE {target_full_name} (
                id INTEGER,
                ts TIMESTAMP
            )
        """)
        conn.commit()
    else:
        # Truncate existing table
        print(f"Truncating existing table {target_full_name}...")
        cursor.execute(f"DELETE FROM {target_full_name}")
        conn.commit()

    # Insert 100 records with different values
    base_timestamp = datetime(2026, 1, 1, 0, 0, 0)
    print("Inserting 100 records...")

    for i in range(1, 101):
        # Each record has a unique ID and incrementing timestamp
        record_id = i
        record_timestamp = base_timestamp + timedelta(seconds=i)

        # Format timestamp for Snowflake: YYYY-MM-DD HH:MM:SS.fff
        ts_str = record_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        insert_sql = f"INSERT INTO {target_full_name} (id, ts) VALUES ({record_id}, '{ts_str}')"
        cursor.execute(insert_sql)
        conn.commit()

        if (i % 10) == 0:
            print(f"  Inserted {i} records...")

    print("Successfully populated table with 100 records")
    print(f"\nSample records:")
    cursor.execute(f"SELECT * FROM {target_full_name} LIMIT 5")
    for row in cursor.fetchall():
        print(f"  ID: {row[0]}, Timestamp: {row[1]}")

    cursor.execute(f"SELECT COUNT(*) FROM {target_full_name}")
    count = cursor.fetchone()[0]
    print(f"\nTotal records in {target_table}: {count}")

    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Populate test_a table with 100 records with different values"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Snowflake host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8088,
        help="Snowflake port (default: 8088)",
    )
    parser.add_argument(
        "--user",
        default="snowflake",
        help="Snowflake user (default: snowflake)",
    )
    parser.add_argument(
        "--password",
        default="snowflake",
        help="Snowflake password (default: snowflake)",
    )
    parser.add_argument(
        "--database",
        default="SNOWFLAKE",
        help="Snowflake database (default: SNOWFLAKE)",
    )
    parser.add_argument(
        "--schema",
        default="PUBLIC",
        help="Snowflake schema (default: PUBLIC)",
    )
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Snowflake warehouse (default: COMPUTE_WH)",
    )

    args = parser.parse_args()

    try:
        populate_test_a(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            schema=args.schema,
            warehouse=args.warehouse,
        )
    except Exception as e:
        print(f"Error: {e}", flush=True)
        exit(1)


if __name__ == "__main__":
    main()
