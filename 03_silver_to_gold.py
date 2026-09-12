from pyspark.sql import functions as F


SILVER_TABLE = (
    "workspace.learning.silver_cross_project_orders"
)

GOLD_TABLE = (
    "workspace.learning.gold_cross_project_sales"
)


print("Reading Silver table...")

silver_df = spark.table(SILVER_TABLE)


print("Creating business aggregation...")

gold_df = (
    silver_df
    .groupBy("city")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.count("*").alias("order_count")
    )
)


print("Writing Gold table...")

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)


print("Gold result:")

gold_df.orderBy(
    F.col("total_sales").desc()
).show()


print(f"Gold completed: {GOLD_TABLE}")