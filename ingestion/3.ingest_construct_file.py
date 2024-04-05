# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

construct_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema = construct_schema)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_ingestion_date_df = add_ingestion_date(constructor_dropped_df) 

# COMMAND ----------

constructor_final_df = constructor_ingestion_date_df.withColumnsRenamed({"constructorId": "constructor_id", "constructorRef": "constructor_ref"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")