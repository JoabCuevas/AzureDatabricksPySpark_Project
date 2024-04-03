# Databricks notebook source
def mount_adls (storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = "Formula1Scope", key= "idcliente")
    tenant_id = dbutils.secrets.get(scope = "Formula1Scope", key= "tenantid")
    client_secret = dbutils.secrets.get(scope = "Formula1Scope", key= "clientsecrets")

    config = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" : client_id,
          "fs.azure.account.oauth2.client.secret" : client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = config),
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("databrickformulacsvdl", "raw")

# COMMAND ----------

mount_adls("databrickformulacsvdl", "presentation")

# COMMAND ----------

mount_adls("databrickformulacsvdl", "processed")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databrickformulacsvdl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/databrickformulacsvdl/demo/circuits.csv"))