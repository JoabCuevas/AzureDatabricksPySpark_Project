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

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.csv(f"{raw_folder_path}/lap_times", schema=lap_times_schema)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_names_changed_df = lap_times_df.withColumnsRenamed({"raceId": "race_id", "driverId" : "driver_id"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_names_changed_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")