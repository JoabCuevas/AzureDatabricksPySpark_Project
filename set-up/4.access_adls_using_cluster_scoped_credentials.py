# Databricks notebook source
dbutils.fs.ls("abfss://demo@databrickformulacsvdl.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickformulacsvdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

