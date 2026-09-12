from pyspark.sql import functions as F

SILVER_TABLE = "workspace.learning.silver_cross_project_orders"
GOLD_TABLE = "workspace.learning.gold_sales_by_city"

silver_df = spark.table(SILVER_TABLE)

gold_df = (
    silver_df
    .groupBy("city")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.count("*").alias("order_count")
    )
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

print("gold_sales_by_city completed")