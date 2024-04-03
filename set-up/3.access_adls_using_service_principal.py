# Databricks notebook source
client_id = dbutils.secrets.get(scope = "Formula1Scope", key= "idcliente")
tenant_id = dbutils.secrets.get(scope = "Formula1Scope", key= "tenantid")
client_secret = dbutils.secrets.get(scope = "Formula1Scope", key= "clientsecrets")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickformulacsvdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickformulacsvdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickformulacsvdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickformulacsvdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickformulacsvdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickformulacsvdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickformulacsvdl.dfs.core.windows.net/circuits.csv"))