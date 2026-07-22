# -*- coding: utf-8 -*-
"""
s3_to_hive_nodup_fast.py
Python 2.7.17 / 3.x compatible.
Fetches CSV data from AWS S3 using credentials managed via .env
and loads deduplicated records into Apache Hive using batch inserts.
"""

import os
import io
import boto3
import pandas as pd
from pyhive import hive
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# -------------------------------
# Configuration from Environment
# -------------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME    = os.getenv("S3_BUCKET_NAME")
OBJECT_KEY     = os.getenv("S3_OBJECT_KEY")

HIVE_HOST      = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT      = int(os.getenv("HIVE_PORT", 10000))
HIVE_USERNAME  = os.getenv("HIVE_USERNAME", "talentum")
HIVE_DATABASE  = os.getenv("HIVE_DATABASE", "default")
HIVE_TABLE     = os.getenv("HIVE_TABLE", "store_locations")


def validate_config():
    """Ensure essential environment variables are present."""
    missing = []
    if not AWS_ACCESS_KEY: missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_KEY: missing.append("AWS_SECRET_ACCESS_KEY")
    if not BUCKET_NAME: missing.append("S3_BUCKET_NAME")
    if not OBJECT_KEY: missing.append("S3_OBJECT_KEY")

    if missing:
        raise ValueError("Missing required environment variables: %s" % ", ".join(missing))


def fetch_csv_from_s3():
    """Fetch CSV file from S3 and return as pandas DataFrame."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    print("[s3] Fetching '%s' from bucket '%s'..." % (OBJECT_KEY, BUCKET_NAME))
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    data = obj["Body"].read().decode("utf-8")
    
    df = pd.read_csv(io.StringIO(data))
    return df


def hive_literal(value):
    """Format standard Python values safely into Hive SQL literals."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 'NULL'
    # Escape backslashes and single quotes to prevent syntax errors
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'{}'".format(escaped)


def store_dataframe_in_hive(df):
    """Store pandas DataFrame into Hive table using batch inserts, skipping duplicates."""
    
    # Active deduplication
    initial_count = len(df)
    df = df.drop_duplicates()
    dedup_count = len(df)
    print("[dedup] Removed %d duplicate rows (%d remaining)" % 
          (initial_count - dedup_count, dedup_count))

    if df.empty:
        print("[hive] No data to insert.")
        return

    conn = hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        username=HIVE_USERNAME,
        database=HIVE_DATABASE
    )
    cursor = conn.cursor()

    # Create Hive table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS {table} (
            store_id STRING,
            country STRING,
            city STRING,
            channel STRING,
            latitude DOUBLE,
            longitude DOUBLE
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """.format(table=HIVE_TABLE)
    
    cursor.execute(create_table_sql)

    rows = df.to_dict(orient='records')
    batch_size = 50
    total_inserted = 0

    print("[hive] Inserting %d unique rows in batches of %d..." % (len(rows), batch_size))

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        selects = []
        
        for row in batch:
            lat_val = str(row.get('latitude')) if pd.notna(row.get('latitude')) else 'NULL'
            lon_val = str(row.get('longitude')) if pd.notna(row.get('longitude')) else 'NULL'
            
            selects.append(
                "SELECT {store_id} AS store_id, {country} AS country, {city} AS city, "
                "{channel} AS channel, {lat} AS latitude, {lon} AS longitude".format(
                    store_id=hive_literal(row.get('store_id')),
                    country=hive_literal(row.get('country')),
                    city=hive_literal(row.get('city')),
                    channel=hive_literal(row.get('channel')),
                    lat=lat_val,
                    lon=lon_val
                )
            )

        insert_sql = "INSERT INTO TABLE {table} {selects}".format(
            table=HIVE_TABLE,
            selects="\nUNION ALL\n".join(selects)
        )
        
        cursor.execute(insert_sql)
        total_inserted += len(batch)
        print("[hive] Inserted rows %d-%d" % (start + 1, start + len(batch)))

    conn.close()
    print("[hive] Data inserted successfully into '%s' (%d total rows)." % 
          (HIVE_TABLE, total_inserted))


def main():
    validate_config()
    df = fetch_csv_from_s3()
    print("[s3] Successfully fetched %d rows." % len(df))
    store_dataframe_in_hive(df)


if __name__ == "__main__":
    main()