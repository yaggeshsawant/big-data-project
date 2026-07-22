from pyspark.sql import SparkSession

def main():
    # Initialize Spark with Hive support
    spark = (
        SparkSession.builder
        .appName("export_hive_to_csv")
        .enableHiveSupport()
        .getOrCreate()
    )

    # ---------------- Config ---------------- #
    hive_table = "default.retail_sales_overview"   # source Hive table
    output_path = "/tmp/retail_sales_export.csv"   # destination CSV file

    # ---------------- Load Hive Table ---------------- #
    df = spark.table(hive_table)
    print(f"Loaded Hive table {hive_table} with {df.count()} rows")

    # ---------------- Export to CSV ---------------- #
    (
        df.coalesce(1)   # optional: write to a single CSV file
          .write
          .mode("overwrite")
          .option("header", "true")   # include column headers
          .csv(output_path)
    )

    print(f"Exported Hive table {hive_table} to CSV at {output_path}")

if __name__ == "__main__":
    main()
