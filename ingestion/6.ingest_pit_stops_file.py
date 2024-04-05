# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json", schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_names_changed_df = pit_stops_df.withColumnsRenamed({"raceId": "race_id", "driverId" : "driver_id"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_names_changed_df)

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")