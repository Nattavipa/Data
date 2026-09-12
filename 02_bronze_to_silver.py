from pyspark.sql import functions as F


BRONZE_TABLE = (
    "workspace.learning.bronze_cross_project_orders"
)

SILVER_TABLE = (
    "workspace.learning.silver_cross_project_orders"
)


print("Reading Bronze table...")

bronze_df = spark.table(BRONZE_TABLE)


print("Cleaning data...")

silver_df = (
    bronze_df
    .dropDuplicates(["order_id"])
    .filter(F.col("amount").isNotNull())
)


print("Running data quality checks...")

duplicate_count = (
    silver_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_amount_count = (
    silver_df
    .filter(F.col("amount").isNull())
    .count()
)


print(f"Duplicate order IDs: {duplicate_count}")
print(f"Null amounts: {null_amount_count}")


print("Writing Silver table...")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)


print(f"Silver completed: {SILVER_TABLE}")