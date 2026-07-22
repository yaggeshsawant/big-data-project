from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# ---------------- Join ---------------- #
def join_sources(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame:
    return (
        sales_df.join(products_df, on="sku_id", how="inner")
                .join(stores_df, on="store_id", how="inner")
    )

# ---------------- Transform ---------------- #
def apply_transformations(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "season",
        when(col("rain_mm") > 5.0, "Monsoon")
        .when(col("temperature") < 8.0, "Winter")
        .when(col("temperature") > 17.0, "Summer")
        .otherwise("Autumn/Spring"),
    ).withColumn(
        "weekday_name",
        when(col("weekday") == 0, "Sunday")
        .when(col("weekday") == 1, "Monday")
        .when(col("weekday") == 2, "Tuesday")
        .when(col("weekday") == 3, "Wednesday")
        .when(col("weekday") == 4, "Thursday")
        .when(col("weekday") == 5, "Friday")
        .otherwise("Saturday"),
    )

    df = (
        df.withColumn("discount_amount", F.round(col("gross_sales") - col("net_sales"), 2))
        .withColumn("total_cost", F.round(col("purchase_cost") * col("units_sold"), 2))
        .withColumn("profit_amount", F.round(col("net_sales") - col("total_cost"), 2))
        .withColumn(
            "revenue_per_unit",
            F.when(col("units_sold") > 0,
                   F.round(col("net_sales") / col("units_sold"), 2))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "discount_band",
            when(col("discount_pct") == 0, "No Discount")
            .when(col("discount_pct") <= 0.10, "0-10%")
            .when(col("discount_pct") <= 0.25, "10-25%")
            .when(col("discount_pct") <= 0.50, "25-50%")
            .otherwise("50%+")
        )
        .withColumn(
            "inventory_risk",
            when(col("stock_out_flag") == 1, "Stock Out")
            .when(col("stock_on_hand") < col("units_sold"), "Understocked")
            .when(col("lead_time_days") >= 14, "Long Lead Time")
            .otherwise("Normal")
        )
    )
    return df

# ---------------- Final Projection ---------------- #
def build_final_df(enriched_df: DataFrame) -> DataFrame:
    return enriched_df.select(
        "date","weekday_name","is_weekend","is_holiday","season",
        "store_id","country","city","channel","sku_id","sku_name","category","subcategory","brand",
        "units_sold","discount_band","discount_amount","promo_flag","gross_sales","net_sales",
        "profit_amount","margin_pct","inventory_risk"
    )

# ---------------- Main ---------------- #
def main():
    spark = SparkSession.builder.appName("retail_sales_transform").enableHiveSupport().getOrCreate()

    # Load cleaned data directly from Hive tables
    products_df = spark.table("default.clean_products")
    stores_df = spark.table("default.clean_stores")
    sales_df = spark.table("default.clean_sales")

    print(f"Loaded products: {products_df.count()} rows")
    print(f"Loaded stores:   {stores_df.count()} rows")
    print(f"Loaded sales:    {sales_df.count()} rows")

    # Join + Transform
    joined_df = join_sources(sales_df, products_df, stores_df)
    enriched_df = apply_transformations(joined_df)
    final_df = build_final_df(enriched_df)

    # Preview
    final_df.printSchema()
    final_df.show(10, truncate=False)

    # Persist curated output into Hive
    final_df.write.mode("overwrite").saveAsTable("default.retail_sales_overview")
    print("✅ Final curated Hive table written: default.retail_sales_overview")

    return final_df

if __name__ == "__main__":
    main()
