from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, upper, initcap
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# ---------------- Spark Session ---------------- #
def get_spark_session(app_name: str = "retail_sales_preprocessing") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )

# ---------------- Schemas ---------------- #
PRODUCTS_SCHEMA = StructType([
    StructField("sku_id", StringType(), True),
    StructField("sku_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("list_price", DoubleType(), True),
])

STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

SALES_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("weekofyear", IntegerType(), True),
    StructField("weekday", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("is_holiday", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("rain_mm", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("sku_id", StringType(), True),
    StructField("units_sold", IntegerType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("promo_flag", IntegerType(), True),
    StructField("gross_sales", DoubleType(), True),
    StructField("net_sales", DoubleType(), True),
    StructField("stock_on_hand", IntegerType(), True),
    StructField("stock_out_flag", IntegerType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("purchase_cost", DoubleType(), True),
    StructField("margin_pct", DoubleType(), True),
])

# ---------------- Config ---------------- #
SALES_HDFS_PATH = "hdfs://localhost:9000/fmcg/raw/landed/*.json"  # wildcard for all JSON files
SALES_HDFS_FORMAT = "json"
STORES_TABLE = "default.store_locations"
PRODUCTS_TABLE = "default.products_details"

# ---------------- Extract ---------------- #
def read_sales_data(spark: SparkSession, path: str, schema: StructType, file_format: str = "json") -> DataFrame:
    if file_format == "json":
        df = spark.read.json(path)
        # Cast explicitly to schema
        for field in schema.fields:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df
    elif file_format == "parquet":
        return spark.read.schema(schema).parquet(path)
    else:
        raise ValueError(f"Unsupported sales data format: {file_format}")

def read_hive_table(spark: SparkSession, table_name: str, expected_schema: StructType) -> DataFrame:
    df = spark.table(table_name)
    for field in expected_schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

# ---------------- Clean ---------------- #
def clean_products(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("sku_id", upper(trim(col("sku_id"))))
        .withColumn("sku_name", trim(col("sku_name")))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("subcategory", initcap(trim(col("subcategory"))))
        .withColumn("brand", trim(col("brand")))
        .withColumn("list_price", F.round(col("list_price"), 2))
        .filter(col("sku_id").isNotNull())
        .filter(col("list_price").isNotNull() & (col("list_price") > 0))
        .dropDuplicates(["sku_id"])
    )

def clean_stores(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("store_id", upper(trim(col("store_id"))))
        .withColumn("country", trim(col("country")))
        .withColumn("city", trim(col("city")))
        .withColumn("channel", trim(col("channel")))
        .filter(col("store_id").isNotNull())
        .filter(col("latitude").between(-90, 90))
        .filter(col("longitude").between(-180, 180))
        .dropDuplicates(["store_id"])
    )

def clean_sales(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("store_id", upper(trim(col("store_id"))))
        .withColumn("sku_id", upper(trim(col("sku_id"))))
        .withColumn("supplier_id", upper(trim(col("supplier_id"))))
        .withColumn("date", col("date").cast(DateType()))
        .dropDuplicates()
        .filter(col("date").isNotNull() & col("store_id").isNotNull() & col("sku_id").isNotNull())
        .filter(col("units_sold") >= 0)
        .filter(col("gross_sales") >= 0)
        .filter(col("net_sales") >= 0)
        .filter(col("stock_on_hand") >= 0)
        .filter(col("purchase_cost") >= 0)
        .na.fill(0, subset=["units_sold","gross_sales","net_sales","stock_on_hand","purchase_cost"])
    )

# ---------------- Main ---------------- #
def main():
    spark = get_spark_session()

    # Extract
    raw_sales_df = read_sales_data(spark, SALES_HDFS_PATH, SALES_SCHEMA, SALES_HDFS_FORMAT)
    raw_stores_df = read_hive_table(spark, STORES_TABLE, STORES_SCHEMA)
    raw_products_df = read_hive_table(spark, PRODUCTS_TABLE, PRODUCTS_SCHEMA)

    print("[debug] raw sales sample:")
    raw_sales_df.select('date','store_id','sku_id','year','month').show(5, truncate=False)

    # Clean
    sales_df = clean_sales(raw_sales_df)
    stores_df = clean_stores(raw_stores_df)
    products_df = clean_products(raw_products_df)

    print(f"Products: {products_df.count()} rows")
    print(f"Stores:   {stores_df.count()} rows")
    print(f"Sales:    {sales_df.count()} rows")

    # Save cleaned outputs locally
    #products_df.repartition(4).write.mode("overwrite").parquet("/tmp/clean_products")
    #stores_df.repartition(4).write.mode("overwrite").parquet("/tmp/clean_stores")
    #sales_df.repartition(8).write.mode("overwrite").parquet("/tmp/clean_sales")

    # Save cleaned outputs into Hive managed tables
    products_df.write.mode("overwrite").saveAsTable("default.clean_products")
    stores_df.write.mode("overwrite").saveAsTable("default.clean_stores")
    sales_df.write.mode("overwrite").saveAsTable("default.clean_sales")

    print("✅ Cleaned data written to local parquet paths and Hive tables.")

if __name__ == "__main__":
    main()
