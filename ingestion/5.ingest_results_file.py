# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/results.json", schema= results_schema)

# COMMAND ----------

results_names_changed_df = results_df.withColumnsRenamed({"resultId": "result_id", "raceId" : "race_id", "driverId": "driver_id", "constructorId": "constructor_id", "positionText": "position_text", "positionOrder" : "position_order", "fastestLapTime" : "fastest_lap_time", "fastestLapSpeed" : "fastest_lap_speed", "fastestLap" : "fastest_lap"}) \
    .drop("statusId") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_final_df = add_ingestion_date(results_names_changed_df)

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/results", partitionBy="race_id")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")