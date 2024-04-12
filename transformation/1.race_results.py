# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnsRenamed({"name": "race_name", "date": "race_date"})

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnsRenamed({"name": "driver_name", "number": "driver_number", "nationality": "driver_nationality"})

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed ("name", "team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

results_drivers_df = drivers_df.join(results_df, results_df.driver_id == drivers_df.driver_id, "inner").select(drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.constructor_id, results_df.race_id, results_df.position)

# COMMAND ----------

results_drivers_constructors_df = results_drivers_df.join(constructor_df.select("constructor_id", "team"), constructor_df.constructor_id == results_drivers_df.constructor_id, "inner")

# COMMAND ----------

results_drivers_constructors_df = results_drivers_constructors_df.drop("constructor_id")

# COMMAND ----------

final_df = results_drivers_constructors_df.join(circuits_races_df, circuits_races_df.race_id == results_drivers_constructors_df.race_id, "inner")

# COMMAND ----------

final_df = add_ingestion_date(final_df) \
    .drop("race_id")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("F1_PRESENTATION.race_results")