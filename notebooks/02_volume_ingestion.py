# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Databricks Ingestion
# MAGIC
# MAGIC Upload `trips.csv`, `locations.csv` and `payment_types.csv`
# MAGIC to a Databricks Volume, then set `BASE_PATH`.

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/taxi_pipeline"

# COMMAND ----------

trips = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/trips.csv")
)

locations = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/locations.csv")
)

payment_types = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/payment_types.csv")
)

# COMMAND ----------

(
    trips.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("trips_raw")
)

(
    locations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("locations_raw")
)

(
    payment_types.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("payment_types_raw")
)

print("Automated ingestion complete.")
