# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("Formula1Scope")

# COMMAND ----------

dbutils.secrets.get(scope = "Formula1Scope", key= "idcliente")

# COMMAND ----------

dbutils.secrets.list("SASTOKEN")