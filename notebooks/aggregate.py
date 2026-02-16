from pyspark.sql.functions import sum, avg

df = spark.table("workspace.silver.demo_clean_sales")

agg_df = df.agg(
    sum("amount").alias("total_sales"),
    avg("amount").alias("avg_sales")
)

agg_df.write.mode("overwrite").saveAsTable("workspace.gold.demo_summary")

print("Aggregation Complete")
