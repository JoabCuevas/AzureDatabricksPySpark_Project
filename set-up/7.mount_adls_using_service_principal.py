# Databricks notebook source
client_id = dbutils.secrets.get(scope = "Formula1Scope", key= "idcliente")
tenant_id = dbutils.secrets.get(scope = "Formula1Scope", key= "tenantid")
client_secret = dbutils.secrets.get(scope = "Formula1Scope", key= "clientsecrets")

# COMMAND ----------

config = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" : client_id,
          "fs.azure.account.oauth2.client.secret" : client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

dbutils.fs.mount(
      source = "abfss://demo@databrickformulacsvdl.dfs.core.windows.net/",
      mount_point = "/mnt/databrickformulacsvdl/demo",
      extra_configs = config)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databrickformulacsvdl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/databrickformulacsvdl/demo/circuits.csv"))

# COMMAND ----------

