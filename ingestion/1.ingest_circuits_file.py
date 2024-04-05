# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True), 
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),  
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", header = True, schema = circuits_schema)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnsRenamed({"circuitId": "circuit_id", "circuitRef": "circuit_ref", "lat": "latitude", "lng": "longitude", "alt": "altitude"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode = "overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")