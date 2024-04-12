-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS F1_PROCESSED
LOCATION "/mnt/databrickformulacsvdl/processed"

-- COMMAND ----------

desc database f1_processed;