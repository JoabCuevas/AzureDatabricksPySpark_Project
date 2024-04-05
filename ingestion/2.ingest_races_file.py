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

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", StringType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/races.csv", header = True, schema = races_schema)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnsRenamed({"raceId": "race_id", "year": "race_year", "circuitId": "circuit_id"}) \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

races_final_df = add_ingestion_date(races_renamed_df) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_final_df.write.parquet(f"{processed_folder_path}/races", mode = "overwrite", partitionBy= "race_year")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")