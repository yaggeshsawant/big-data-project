# -*- coding: utf-8 -*-
"""
hive_to_s3_export_csv.py
Reads curated Hive table and writes it to AWS S3 in CSV format.
Dependencies:
    pyspark with hadoop-aws and aws-java-sdk jars
"""

import os
import sys
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import SparkSession
import tempfile
import shutil

# -------------------------------
# AWS S3 Configuration
# -------------------------------
# Prefer credentials from environment variables or IAM role. Do NOT hardcode keys.
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_KEY")
AWS_REGION     = os.environ.get("AWS_REGION", "ap-south-1")
BUCKET_NAME    = "retail-sales-overview-bucket"
S3_OUTPUT_PATH = "s3a://{bucket}/retail_sales_overview_csv/".format(bucket=BUCKET_NAME)

# -------------------------------
# Hive Configuration
# -------------------------------
HIVE_TABLE = "default.retail_sales_overview"

def main():
    # Initialize Spark with Hive support
    spark = (
        SparkSession.builder
        .appName("hive_to_s3_export_csv")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Configure AWS credentials for S3 access
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # Ensure the S3A implementation is set (requires hadoop-aws jar on classpath)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Use SimpleAWSCredentialsProvider when explicit keys are provided
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    else:
        print("⚠️  No AWS credentials found in environment; relying on instance/IAM role or default provider chain.")

    # Endpoint and compatibility
    hadoop_conf.set("fs.s3a.endpoint", "s3.{region}.amazonaws.com".format(region=AWS_REGION))
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")

    # Load Hive table
    df = spark.table(HIVE_TABLE)
    print("✅ Loaded Hive table: {table}".format(table=HIVE_TABLE))
    print("Row count: {count}".format(count=df.count()))
    df.printSchema()

    # Write to S3 in CSV format with header
    # Fallback approach: write CSV locally then upload to S3 with boto3
    tmp_dir = tempfile.mkdtemp(prefix="retail_csv_")
    try:
        local_path = tmp_dir + "/retail_sales_overview_csv"
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(local_path)
        )
        print("✅ Data written locally to: {lp}".format(lp=local_path))

        # Upload all files from local_path to S3 using boto3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        )

        # Find the CSV file(s) produced (Spark writes part-... files)
        uploaded = []
        for root, dirs, files in __import__("os").walk(local_path):
            for fname in files:
                if fname.endswith(".csv") or fname.startswith("part-"):
                    local_file = root + "/" + fname
                    s3_key = "retail_sales_overview_csv/" + fname
                    try:
                        s3.upload_file(local_file, BUCKET_NAME, s3_key)
                        uploaded.append(s3_key)
                    except (BotoCoreError, ClientError) as be:
                        print("❌ Failed to upload {f}: {err}".format(f=local_file, err=be))
                        sys.exit(1)

        print("✅ Uploaded files to s3://{bucket}/:".format(bucket=BUCKET_NAME))
        for u in uploaded:
            print(" - {k}".format(k=u))

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
