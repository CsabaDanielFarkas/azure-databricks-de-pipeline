# Databricks notebook source
spark.sql("""
CREATE SCHEMA IF NOT EXISTS dbw_nyc_taxi_de.bronze
""")

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS dbw_nyc_taxi_de.bronze.raw
""")

# COMMAND ----------

path = "/Volumes/dbw_nyc_taxi_de/bronze/raw/yellow_tripdata_2026-01.parquet"
df = spark.read.parquet(path)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(f"Rows: {df.count()}")
print(f"Columns: {len(df.columns)}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
bronze_df = (
    df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("yellow_tripdata_2026-01.parquet"))
)

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dbw_nyc_taxi_de.bronze.yellow_taxi")
)

# COMMAND ----------

bronze = spark.table("dbw_nyc_taxi_de.bronze.yellow_taxi")

display(bronze)

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM dbw_nyc_taxi_de.bronze.yellow_taxi
        LIMIT 20
    """)
)

# COMMAND ----------

