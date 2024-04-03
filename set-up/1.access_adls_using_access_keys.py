# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope = "Formula1Scope", key= "formula1-account-key")

# COMMAND ----------

# Set the Azure configuration using the provided key
spark.conf.set("fs.azure.account.key.databrickformulacsvdl.dfs.core.windows.net", formula1_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickformulacsvdl.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickformulacsvdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

