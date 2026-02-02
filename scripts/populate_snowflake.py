#!/usr/bin/env python3
"""
Populate the SNOWFLAKE.PUBLIC.test_source table with 100 records with different values.
Uses the REST API v2 to connect to the Snowflake emulator.

Usage:
    python populate_snowflake.py [--host HOST] [--port PORT]

Example:
    python populate_snowflake.py --host localhost --port 8081

Prerequisites:
    1. Snowflake emulator running
    2. Port forwarding if running in Kubernetes:
       kubectl port-forward -n snowflake-emulator svc/snowflake-emulator-external 8081:8081 &
"""

import argparse
import json
import time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen


def populate_test_source(host: str, port: int) -> None:
    """Populate test_source table with 100 records with different values using REST API."""
    base_url = f"http://{host}:{port}/api/v2"
    database = "memory"  # Use the default database in snowflake-emulator
    schema = "main"     # DuckDB default schema
    table = "test_source"
    
    print(f"Connecting to Snowflake emulator at {host}:{port}")

    # Drop table if it exists
    print(f"Dropping existing table {table}...")
    drop_sql = f"DROP TABLE IF EXISTS {table}"
    request_data = json.dumps({
        "statement": drop_sql,
        "database": database
    }).encode()
    req = Request(
        f"{base_url}/statements",
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        response = urlopen(req)
        print(f"  Table dropped: {response.status}")
    except Exception as e:
        print(f"  Drop skipped: {e}")

    # Create fresh table
    print(f"Creating table {table}...")
    create_table_sql = f"CREATE TABLE {table} (id INTEGER, ts TIMESTAMP)"
    
    request_data = json.dumps({
        "statement": create_table_sql,
        "database": database
    }).encode()
    req = Request(
        f"{base_url}/statements",
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    response = urlopen(req)
    result = json.loads(response.read())
    print(f"  Table created: {response.status}")

    # Insert 100 records
    base_timestamp = datetime(2026, 1, 1, 0, 0, 0)
    print("Inserting 100 records...")

    for i in range(1, 101):
        record_id = i
        record_timestamp = base_timestamp + timedelta(seconds=i)
        ts_str = record_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        insert_sql = f"INSERT INTO {table} (id, ts) VALUES ({record_id}, '{ts_str}')"

        request_data = json.dumps({
            "statement": insert_sql,
            "database": database
        }).encode()
        req = Request(
            f"{base_url}/statements",
            data=request_data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        response = urlopen(req)
        
        if (i % 10) == 0:
            print(f"  Inserted {i} records...")
    
    print("Successfully populated table with 100 records")
    
    # Verify data
    print(f"\nSample records:")
    select_sql = f"SELECT * FROM {table} LIMIT 5"
    request_data = json.dumps({
        "statement": select_sql,
        "database": database
    }).encode()
    req = Request(
        f"{base_url}/statements",
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    response = urlopen(req)
    result = json.loads(response.read())
    
    if "data" in result and result["data"]:
        for row in result["data"]:
            print(f"  ID: {row[0]}, Timestamp: {row[1]}")

    # Count records
    count_sql = f"SELECT COUNT(*) FROM {table}"
    request_data = json.dumps({
        "statement": count_sql,
        "database": database
    }).encode()
    req = Request(
        f"{base_url}/statements",
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    response = urlopen(req)
    result = json.loads(response.read())

    if "data" in result and result["data"]:
        count = result["data"][0][0]
        print(f"\nTotal records in {table}: {count}")


def main():
    parser = argparse.ArgumentParser(
        description="Populate test_source table with 100 records using REST API v2"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Snowflake host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Snowflake port (default: 8081)",
    )
    
    args = parser.parse_args()
    
    try:
        populate_test_source(host=args.host, port=args.port)
    except Exception as e:
        print(f"Error: {e}", flush=True)
        exit(1)


if __name__ == "__main__":
    main()
