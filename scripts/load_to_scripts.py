#!/usr/bin/env python3
"""
load_to_postgres.py
Reads CSV -> cleans -> creates table if not exists -> appends with COPY.

Usage:
  python scripts/load_to_postgres.py --csv data/nashville_housing.csv
Or in Docker Compose:
  docker compose run --rm ingester python scripts/load_to_postgres.py --csv /app/data/nashville_housing.csv
"""
import argparse
import os
import re
import time
from io import StringIO

import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def sanitize(col):
    s = str(col).strip().lower()
    s = re.sub(r'[^0-9a-z]+', '_', s)
    s = re.sub(r'__+', '_', s).strip('_')
    if s == '' or s[0].isdigit():
        s = 'col_' + s
    return s

def pg_type_from_series(s: pd.Series):
    if pd.api.types.is_integer_dtype(s):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(s):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_bool_dtype(s):
        return 'BOOLEAN'
    try:
        non_null = s.dropna()
        if not non_null.empty:
            maybe = pd.to_datetime(non_null.head(50), errors='coerce')
            if maybe.notnull().any():
                return 'DATE'
    except Exception:
        pass
    return 'TEXT'

def generate_create_table_sql(df: pd.DataFrame, schema: str, table: str):
    cols = df.columns.tolist()
    parts = []
    for c in cols:
        typ = pg_type_from_series(df[c])
        name = sanitize(c)
        parts.append(f"{name} {typ}")
    columns_sql = ",\n    ".join(parts)
    ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n    id SERIAL PRIMARY KEY,\n    {columns_sql}\n);\n"
    return ddl

def wait_for_db(dsn, max_retries=20, wait_seconds=3):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            print("[ok] db is reachable")
            return True
        except Exception as e:
            print(f"[waiting] db not ready yet ({i+1}/{max_retries}): {e}")
            time.sleep(wait_seconds)
    raise RuntimeError("Database not reachable after retries")

def table_exists(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s
            );
            """, (schema, table)
        )
        return cur.fetchone()[0]

def main():
    parser = argparse.ArgumentParser(description="Load CSV into Postgres (append if table exists).")
    parser.add_argument("--csv", help="path to csv file", required=False, default=os.environ.get('CSV_PATH', 'data/nashville_housing.csv'))
    parser.add_argument("--schema", default=os.environ.get('PG_SCHEMA','public'))
    parser.add_argument("--table", default=os.environ.get('PG_TABLE','nashville_housing'))
    args = parser.parse_args()

    csv_path = args.csv
    schema = args.schema
    table = args.table

    if not os.path.exists(csv_path):
        raise SystemExit(f"CSV file not found at {csv_path} -- please place it there and retry.")

    print("Reading CSV:", csv_path)
    df = pd.read_csv(csv_path, low_memory=False)

    # drop unnamed index-ish columns
    df = df[[c for c in df.columns if not str(c).lower().startswith('unnamed')]]

    # sanitize column names
    new_columns = [sanitize(c) for c in df.columns.tolist()]
    df.columns = new_columns

    # try parsing a sale_date column if present
    if 'sale_date' in df.columns:
        print("Parsing 'sale_date' to date")
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.date

    # connect to postgres using environment variables (correct keys)
    pg_host = os.environ.get('PGHOST','localhost')
    pg_port = os.environ.get('PGPORT','5432')
    pg_db = os.environ.get('PGDATABASE','postgres')
    pg_user = os.environ.get('PGUSER','postgres')
    pg_password = os.environ.get('PGPASSWORD','postgres')

    print(f"Will connect to host={pg_host} port={pg_port} db={pg_db} user={pg_user}")
    dsn = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_password}"
    print("Waiting for Postgres to be ready...")
    wait_for_db(dsn)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        if not table_exists(conn, schema, table):
            print(f"Table {schema}.{table} does not exist. Creating...")
            ddl = generate_create_table_sql(df, schema, table)
            print("DDL:\n", ddl)
            with conn.cursor() as cur:
                cur.execute(ddl)
                conn.commit()
            print("Table created.")
        else:
            print(f"Table {schema}.{table} exists -- will append rows.")

        # COPY from a buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d')
        csv_buffer.seek(0)

        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH CSV HEADER").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, df.columns.tolist()))
        )

        with conn.cursor() as cur:
            print("Starting COPY ... (this will append rows)")
            cur.copy_expert(copy_sql.as_string(conn), csv_buffer)
            conn.commit()
            print("COPY finished, rows appended.")

    except Exception as e:
        conn.rollback()
        print("Error during load:", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
