#!/usr/bin/env python
# coding: utf-8

import argparse


import duckdb


def main(params) -> None:
    # Connect to DuckDB
    conn = duckdb.connect(database=":memory:", read_only=False)

    # Load the Parquet file into DuckDB
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(
        f"CREATE TABLE {params.table_name} AS SELECT * FROM read_parquet('{params.url}')"
    )

    # Connect DuckDB to PostgreSQL
    pg_conn = f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}"
    # con.execute(f"INSTALL postgres; LOAD postgres;")  # Ensure PostgreSQL extension is available
    conn.execute(f"ATTACH '{pg_conn}' AS pg_db (TYPE postgres);")

    # Transfer Data from DuckDB to PostgreSQL
    conn.execute(
        f"CREATE TABLE pg_db.{params.table_name} AS SELECT * FROM {params.table_name}"
    )

    print("✅ Data successfully saved to PostgreSQL!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()

    main(args)
