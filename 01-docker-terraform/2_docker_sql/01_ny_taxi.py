import marimo

__generated_with = "0.11.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import polars as pl
    from sqlalchemy import create_engine

    def duck_it():
        # Connect to DuckDB
        con = duckdb.connect(database=":memory:", read_only=False)

        # Load the Parquet file into DuckDB
        parquet_file = "01-docker-terraform/2_docker_sql/yellow_tripdata_2021-01.parquet"
        con.execute(f"CREATE TABLE yellow_tripdata AS SELECT * FROM read_parquet('{parquet_file}')")

        # Connect DuckDB to PostgreSQL
        pg_conn_str = "postgresql://root:root@localhost:5432/ny_taxi"
        con.execute(f"INSTALL postgres; LOAD postgres;")  # Ensure PostgreSQL extension is available
        con.execute(f"ATTACH '{pg_conn_str}' AS pg_db (TYPE postgres);")

        # Transfer Data from DuckDB to PostgreSQL
        con.execute("CREATE TABLE pg_db.yellow_tripdata AS SELECT * FROM yellow_tripdata")

        print("✅ Data successfully saved to PostgreSQL!")

    def polar_it():
        # Define file path and PostgreSQL connection string
        parquet_file = "01-docker-terraform/2_docker_sql/yellow_tripdata_2021-01.parquet"
        pg_conn_str = "postgresql://root:root@localhost:5432/ny_taxi"

        # Load Parquet file into a Polars DataFrame
        df = pl.read_parquet(parquet_file)

        # Connect to PostgreSQL using SQLAlchemy
        engine = create_engine(pg_conn_str)

        # Write DataFrame to PostgreSQL
        df.write_database(table_name="yellow_tripdata", connection=engine)

        print("✅ Data successfully saved to PostgreSQL!")

    def delete_it():
        con = duckdb.connect(database=":memory:", read_only=False)
        pg_conn_str = "postgresql://root:root@localhost:5432/ny_taxi"
        con.execute(f"ATTACH '{pg_conn_str}' AS pg_db (TYPE postgres);")

        # Transfer Data from DuckDB to PostgreSQL
        con.execute("DROP TABLE pg_db.yellow_tripdata")

        print("✅ Data successfully dropped to PostgreSQL!")
    return create_engine, delete_it, duck_it, duckdb, pl, polar_it


@app.cell
def _(delete_it, polar_it):
    from timeit import default_timer as timer
    from datetime import timedelta

    start = timer()
    polar_it()
    end = timer()
    print(timedelta(seconds=end-start))
    delete_it()
    return end, start, timedelta, timer


if __name__ == "__main__":
    app.run()
