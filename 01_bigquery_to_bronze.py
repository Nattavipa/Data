SOURCE_TABLE = (
    "`cross_project_bigquery_catalog`.learning.orders"
)

BRONZE_TABLE = (
    "workspace.learning.bronze_cross_project_orders"
)


# Make sure target schema exists
spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.learning
""")


print("Reading BigQuery source...")

source_df = spark.table(SOURCE_TABLE)

print("Source schema:")
source_df.printSchema()


print("Writing Bronze table...")

(
    source_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)


print(f"Bronze completed: {BRONZE_TABLE}")