from pyspark.sql.functions import rand

df = spark.range(100).withColumn("amount", (rand()*1000).cast("int"))
df.write.mode("overwrite").saveAsTable("workspace.bronze.demo_raw_sales")

print("Ingestion Complete")
