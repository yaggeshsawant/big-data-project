# -*- coding: utf-8 -*-
"""
snowflake_to_hive.py
Python 2.7.17 compatible.
Fetches SKU data from Snowflake and stores it into Hive.
Dependencies:
    pip install snowflake-connector-python==1.8.4 pandas==0.24.2 pyhive==0.6.1
"""

import os  
import io
import pandas as pd
import snowflake.connector
from pyhive import hive
from dotenv import load_dotenv
import sys

# Load environment variables from a .env file (if present)
load_dotenv()

# -------------------------------
# Snowflake Connection Parameters
# -------------------------------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER_ID")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD_KEY")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# Validate required Snowflake environment variables early with clear message
missing = [
    name for name, val in (
        ("SNOWFLAKE_USER_ID", SNOWFLAKE_USER),
        ("SNOWFLAKE_PASSWORD_KEY", SNOWFLAKE_PASSWORD),
        ("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT),
        ("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE),
        ("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE),
        ("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA),
        ("SNOWFLAKE_TABLE", SNOWFLAKE_TABLE),
    ) if not val
]
if missing:
    print("Missing required Snowflake environment variables: {}".format(
        ", ".join(missing)
    ))
    print("Create a .env file or export these variables before running the script.")
    sys.exit(1)

# -------------------------------
# Hive Connection Parameters
# -------------------------------
HIVE_HOST      = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT      = int(os.getenv("HIVE_PORT", 10000))
HIVE_USERNAME  = os.getenv("HIVE_USERNAME", "talentum")
HIVE_DATABASE  = os.getenv("HIVE_DATABASE", "default")
HIVE_TABLE     = os.getenv("HIVE_TABLE", "products_details")


def fetch_from_snowflake():
    """Fetch SKU data from Snowflake into Pandas DataFrame."""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM {table};".format(table=SNOWFLAKE_TABLE))

    try:
        df = cursor.fetch_pandas_all()
    except Exception:
        # Fallback when pyarrow is not available.
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

    cursor.close()
    conn.close()
    print("[snowflake] fetched %d rows" % len(df))
    return df

def store_in_hive(df):
    """Store DataFrame rows into Hive table."""
    print("[hive] connecting to Hive {}:{}".format(HIVE_HOST, HIVE_PORT))
    hive_conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT,
                                username=HIVE_USERNAME, database=HIVE_DATABASE)
    hive_cursor = hive_conn.cursor()

    print("[hive] ensuring table exists: {}".format(HIVE_TABLE))
    hive_cursor.execute("""
        CREATE TABLE IF NOT EXISTS {table} (
            sku_id STRING,
            sku_name STRING,
            category STRING,
            subcategory STRING,
            brand STRING,
            list_price DOUBLE
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """.format(table=HIVE_TABLE))

    def hive_literal(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 'NULL'
        return "'{}'".format(str(value).replace("\\", "\\\\").replace("'", "\\'"))

    print("[hive] inserting %d rows" % len(df))
    rows = df.to_dict(orient='records')
    batch_size = 50
    total_inserted = 0

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        selects = []
        for row in batch:
            price = row.get('LIST_PRICE')
            if price is None or (isinstance(price, float) and pd.isna(price)):
                price_literal = 'NULL'
            else:
                price_literal = str(price)

            selects.append(
                "SELECT {sku_id} AS sku_id, {sku_name} AS sku_name, {category} AS category, "
                "{subcategory} AS subcategory, {brand} AS brand, {price} AS list_price".format(
                    sku_id=hive_literal(row.get('SKU_ID')),
                    sku_name=hive_literal(row.get('SKU_NAME')),
                    category=hive_literal(row.get('CATEGORY')),
                    subcategory=hive_literal(row.get('SUBCATEGORY')),
                    brand=hive_literal(row.get('BRAND')),
                    price=price_literal
                )
            )

        insert_sql = "INSERT INTO TABLE {table} {selects}".format(
            table=HIVE_TABLE,
            selects="\nUNION ALL\n".join(selects)
        )
        hive_cursor.execute(insert_sql)
        total_inserted += len(batch)
        print("[hive] inserted rows %d-%d" % (start + 1, start + len(batch)))

    hive_conn.close()
    print("[hive] data inserted successfully into Hive table (%d rows)" % total_inserted)

def main():
    df = fetch_from_snowflake()
    store_in_hive(df)

if __name__ == "__main__":
    main()
