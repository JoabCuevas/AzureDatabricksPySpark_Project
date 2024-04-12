# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

constructors_standings_df = results_df.groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructors_standings_df.withColumn("rank", rank().over(constructors_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("F1_PRESENTATION.constructor_standings")