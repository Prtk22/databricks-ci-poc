from pyspark.sql.functions import col

df = spark.table("workspace.bronze.demo_raw_sales")

clean_df = df.filter(col("amount") > 100)

clean_df.write.mode("overwrite").saveAsTable("workspace.silver.demo_clean_sales")

print("Transformation Complete")
