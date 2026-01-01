#!/usr/bin/env python3
"""
Populate test_a table with 100 records with different values.

Usage:
    python populate_test_a.py [--host HOST] [--port PORT] [--user USER]

Example:
    python populate_test_a.py --host localhost --port 8080 --user dagster
"""

import argparse
from datetime import datetime, timedelta

import trino


def populate_test_a(host: str, port: int, user: str) -> None:
    """Populate test_a table with 100 records with different values."""
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
    )
    cursor = conn.cursor()

    target_catalog = "lakehouse"
    target_schema = "test"
    target_table = "test_a"
    target_full_name = f"{target_catalog}.{target_schema}.{target_table}"

    print(f"Populating {target_full_name} with 100 records...")

    # Check if table exists, if not create it
    cursor.execute(f"""
        SELECT table_name FROM {target_catalog}.information_schema.tables
        WHERE table_catalog = '{target_catalog}'
        AND table_schema = '{target_schema}'
        AND table_name = '{target_table}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    if not table_exists:
        print(f"Creating table {target_full_name}...")
        cursor.execute(f"""
            CREATE TABLE {target_full_name} (
                id BIGINT,
                ts TIMESTAMP
            )
        """)
        cursor.fetchall()
    else:
        # Truncate existing table
        print(f"Truncating existing table {target_full_name}...")
        cursor.execute(f"DELETE FROM {target_full_name}")
        cursor.fetchall()

    # Insert 100 records with different values
    base_timestamp = datetime(2026, 1, 1, 0, 0, 0)
    print("Inserting 100 records...")

    for i in range(1, 101):
        # Each record has a unique ID and incrementing timestamp
        record_id = i
        record_timestamp = base_timestamp + timedelta(seconds=i)

        # Format timestamp for Trino: YYYY-MM-DD HH:MM:SS.fff
        ts_str = record_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        insert_sql = f"INSERT INTO {target_full_name} (id, ts) VALUES ({record_id}, TIMESTAMP '{ts_str}')"
        cursor.execute(insert_sql)
        cursor.fetchall()

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
        help="Trino host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Trino port (default: 8080)",
    )
    parser.add_argument(
        "--user",
        default="dagster",
        help="Trino user (default: dagster)",
    )

    args = parser.parse_args()

    try:
        populate_test_a(
            host=args.host,
            port=args.port,
            user=args.user,
        )
    except Exception as e:
        print(f"Error: {e}", flush=True)
        exit(1)


if __name__ == "__main__":
    main()

