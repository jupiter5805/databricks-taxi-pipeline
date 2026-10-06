# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Databricks Pipeline
# MAGIC
# MAGIC This notebook demonstrates an end-to-end Databricks pipeline using:
# MAGIC
# MAGIC - Spark / PySpark
# MAGIC - Python
# MAGIC - SQL
# MAGIC - Delta tables
# MAGIC - Databricks visualisations
# MAGIC
# MAGIC Before running, upload the three generated files as tables named:
# MAGIC
# MAGIC - `trips_raw`
# MAGIC - `locations_raw`
# MAGIC - `payment_types_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Why Databricks?
# MAGIC
# MAGIC Databricks provides a collaborative environment for building data and AI
# MAGIC workloads around Apache Spark. In one workspace we can ingest data, clean
# MAGIC and transform it with PySpark, query it with SQL, persist Delta tables and
# MAGIC create visualisations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query the raw tables with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM trips_raw LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM locations_raw LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM payment_types_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load the raw tables with Spark

# COMMAND ----------

from pyspark.sql import functions as F

trips_raw = spark.table("trips_raw")
locations_raw = spark.table("locations_raw")
payments_raw = spark.table("payment_types_raw")

print("Trips:", trips_raw.count())
print("Locations:", locations_raw.count())
print("Payment types:", payments_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect nulls

# COMMAND ----------

display(
    trips_raw.select(
        [
            F.sum(
                F.col(column).isNull().cast("int")
            ).alias(column)
            for column in trips_raw.columns
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean and cast the trip data

# COMMAND ----------

trips = (
    trips_raw
    .withColumn("trip_id", F.col("trip_id").cast("long"))
    .withColumn("vendor_id", F.col("vendor_id").cast("int"))
    .withColumn(
        "pickup_datetime",
        F.to_timestamp("pickup_datetime"),
    )
    .withColumn(
        "dropoff_datetime",
        F.to_timestamp("dropoff_datetime"),
    )
    .withColumn(
        "passenger_count",
        F.col("passenger_count").cast("int"),
    )
    .withColumn(
        "trip_distance",
        F.col("trip_distance").cast("double"),
    )
    .withColumn(
        "pickup_location_id",
        F.col("pickup_location_id").cast("int"),
    )
    .withColumn(
        "dropoff_location_id",
        F.col("dropoff_location_id").cast("int"),
    )
    .withColumn(
        "payment_type_id",
        F.col("payment_type_id").cast("int"),
    )
    .withColumn(
        "fare_amount",
        F.col("fare_amount").cast("double"),
    )
    .withColumn(
        "tip_amount",
        F.col("tip_amount").cast("double"),
    )
    .withColumn(
        "tolls_amount",
        F.col("tolls_amount").cast("double"),
    )
    .withColumn(
        "total_amount",
        F.col("total_amount").cast("double"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Feature engineering with PySpark

# COMMAND ----------

trips_enriched = (
    trips
    .filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
        & F.col("trip_distance").isNotNull()
        & F.col("total_amount").isNotNull()
    )
    .filter(
        F.col("dropoff_datetime")
        > F.col("pickup_datetime")
    )
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("total_amount") >= 0)
    .withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("dropoff_datetime")
            - F.unix_timestamp("pickup_datetime")
        ) / 60.0,
    )
    .withColumn(
        "average_speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            F.col("trip_distance")
            / (F.col("trip_duration_minutes") / 60.0),
        ),
    )
    .withColumn(
        "pickup_date",
        F.to_date("pickup_datetime"),
    )
    .withColumn(
        "pickup_hour",
        F.hour("pickup_datetime"),
    )
    .withColumn(
        "day_of_week",
        F.date_format("pickup_datetime", "EEEE"),
    )
    .withColumn(
        "fare_per_mile",
        F.when(
            F.col("trip_distance") > 0,
            F.col("fare_amount") / F.col("trip_distance"),
        ),
    )
    .withColumn(
        "tip_percentage",
        F.when(
            F.col("fare_amount") > 0,
            F.col("tip_amount")
            / F.col("fare_amount") * 100,
        ),
    )
    .withColumn(
        "distance_band",
        F.when(
            F.col("trip_distance") <= 2,
            "Short",
        )
        .when(
            F.col("trip_distance") <= 5,
            "Medium",
        )
        .when(
            F.col("trip_distance") <= 10,
            "Long",
        )
        .otherwise("Very Long"),
    )
    .withColumn(
        "time_band",
        F.when(
            F.col("pickup_hour").between(0, 5),
            "Overnight",
        )
        .when(
            F.col("pickup_hour").between(6, 11),
            "Morning",
        )
        .when(
            F.col("pickup_hour").between(12, 17),
            "Afternoon",
        )
        .otherwise("Evening"),
    )
    .filter(
        F.col("trip_duration_minutes").between(1, 240)
    )
    .filter(
        F.col("average_speed_mph").isNull()
        | F.col("average_speed_mph").between(0, 80)
    )
)

print("Cleaned trips:", trips_enriched.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Join related datasets

# COMMAND ----------

pickup_locations = (
    locations_raw
    .select(
        F.col("location_id").cast("int").alias(
            "pickup_location_id"
        ),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
)

dropoff_locations = (
    locations_raw
    .select(
        F.col("location_id").cast("int").alias(
            "dropoff_location_id"
        ),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
)

payments = (
    payments_raw
    .select(
        F.col("payment_type_id").cast("int").alias(
            "payment_type_id"
        ),
        F.col("payment_type"),
    )
)

trips_final = (
    trips_enriched
    .join(
        pickup_locations,
        on="pickup_location_id",
        how="left",
    )
    .join(
        dropoff_locations,
        on="dropoff_location_id",
        how="left",
    )
    .join(
        payments,
        on="payment_type_id",
        how="left",
    )
)

display(trips_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Persist transformed data as a Delta table

# COMMAND ----------

(
    trips_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("trips_clean")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. SQL transformation check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     distance_band,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     ROUND(AVG(trip_distance), 2) AS avg_distance,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_total_amount
# MAGIC FROM trips_clean
# MAGIC GROUP BY distance_band
# MAGIC ORDER BY avg_distance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Visualisation 1 — Trips by hour
# MAGIC
# MAGIC Click **+ > Visualization**, choose a **Line chart**,
# MAGIC use `pickup_hour` on X and `trip_count` on Y.

# COMMAND ----------

hourly_demand = spark.sql(
    """
    SELECT
        pickup_hour,
        COUNT(*) AS trip_count
    FROM trips_clean
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """
)

display(hourly_demand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Visualisation 2 — Top pickup zones
# MAGIC
# MAGIC Use a **Bar chart** with `pickup_zone` and `trip_count`.

# COMMAND ----------

top_pickup_zones = spark.sql(
    """
    SELECT
        pickup_zone,
        pickup_borough,
        COUNT(*) AS trip_count,
        ROUND(AVG(total_amount), 2) AS avg_total_amount
    FROM trips_clean
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_zone, pickup_borough
    ORDER BY trip_count DESC
    LIMIT 15
    """
)

display(top_pickup_zones)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Visualisation 3 — Payment methods
# MAGIC
# MAGIC Use a **Bar chart** or **Pie chart**.

# COMMAND ----------

payment_summary = spark.sql(
    """
    SELECT
        COALESCE(payment_type, 'Unmapped') AS payment_type,
        COUNT(*) AS trip_count,
        ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
    FROM trips_clean
    GROUP BY payment_type
    ORDER BY trip_count DESC
    """
)

display(payment_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Visualisation 4 — Revenue by pickup borough
# MAGIC
# MAGIC Use a **Bar chart** with `pickup_borough` and `total_revenue`.

# COMMAND ----------

borough_revenue = spark.sql(
    """
    SELECT
        pickup_borough,
        COUNT(*) AS trip_count,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_trip_value
    FROM trips_clean
    WHERE pickup_borough IS NOT NULL
    GROUP BY pickup_borough
    ORDER BY total_revenue DESC
    """
)

display(borough_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Create reusable SQL views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_hourly_demand AS
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     COUNT(*) AS trip_count
# MAGIC FROM trips_clean
# MAGIC GROUP BY pickup_hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_top_pickup_zones AS
# MAGIC SELECT
# MAGIC     pickup_zone,
# MAGIC     pickup_borough,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_trip_value
# MAGIC FROM trips_clean
# MAGIC WHERE pickup_zone IS NOT NULL
# MAGIC GROUP BY pickup_zone, pickup_borough;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_payment_summary AS
# MAGIC SELECT
# MAGIC     COALESCE(payment_type, 'Unmapped') AS payment_type,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
# MAGIC FROM trips_clean
# MAGIC GROUP BY payment_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Conclusion
# MAGIC
# MAGIC This pipeline:
# MAGIC
# MAGIC 1. ingested three related datasets
# MAGIC 2. cleaned raw trip records with PySpark
# MAGIC 3. engineered analytical features
# MAGIC 4. joined foreign-key dimensions
# MAGIC 5. persisted an analytics-ready Delta table
# MAGIC 6. queried the data using SQL
# MAGIC 7. created reusable views
# MAGIC 8. produced datasets for native Databricks visualisations
