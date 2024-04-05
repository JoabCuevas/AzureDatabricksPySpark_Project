# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date (dataframe):
    output_df = dataframe.withColumn("ingestion_date", current_timestamp())
    return output_df