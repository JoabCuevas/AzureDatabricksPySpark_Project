# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields=[
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path/{v_file_date}/drivers.json", schema= driver_schema)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

drivers_changed_names_df = drivers_dropped_df.withColumnsRenamed({"driverID": "driver_id", "driverRef": "driver_ref"}) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_changed_names_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("F1_PROCESSED.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")