# Databricks notebook source
df = spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))

# COMMAND ----------

df.columns

# COMMAND ----------

display(
    df.select(
        "tpep_pickup_datetime",
        "pickup_zip",
        "dropoff_zip",
        "trip_distance",
        "fare_amount"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

clean_df=(df
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") > 0))

display(clean_df.limit(20))

# COMMAND ----------

print("Original rows:", df.count())
print("Valid rows:", clean_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE fare_amount < 0
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_zip,
# MAGIC     COUNT(*) AS number_of_trips,
# MAGIC     ROUND(AVG(fare_amount),2) AS avg_fare,
# MAGIC     ROUND(AVG(trip_distance),2) AS avg_distance
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance>0 AND fare_amount>0
# MAGIC GROUP BY pickup_zip
# MAGIC ORDER BY number_of_trips DESC
# MAGIC LIMIT 10;

# COMMAND ----------

ROUN