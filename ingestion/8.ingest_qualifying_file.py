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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/qualifying", schema=qualifying_schema, multiLine=True)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_names_changed_df = qualifying_df.withColumnsRenamed({"qualifyId": "qualify_id", "driverId" : "driver_id", "raceId": "race_id", "constructorId": "constructor_id"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_names_changed_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")