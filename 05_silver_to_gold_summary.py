from pyspark.sql import functions as F

SILVER_TABLE = "workspace.learning.silver_cross_project_orders"
GOLD_TABLE = "workspace.learning.gold_order_summary"

silver_df = spark.table(SILVER_TABLE)

summary_df = (
    silver_df
    .agg(
        F.sum("amount").alias("total_sales"),
        F.avg("amount").alias("avg_order_amount"),
        F.count("*").alias("total_orders")
    )
)

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

print("gold_order_summary completed")