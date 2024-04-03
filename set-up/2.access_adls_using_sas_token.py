# Databricks notebook source
SAS_TOKEN = dbutils.secrets.get(scope = "SASTOKEN", key= "sas")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickformulacsvdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickformulacsvdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickformulacsvdl.dfs.core.windows.net", SAS_TOKEN)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickformulacsvdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickformulacsvdl.dfs.core.windows.net/circuits.csv"))