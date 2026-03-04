# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

raw_root = "/Volumes/lakehouse/adventureworks_multisales/raw_zone"

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(f"{raw_root}/*.csv")
      .withColumn("ingest_time", current_timestamp())
      .withColumn("source_file", col("_metadata.file_path"))
)

display(df.select("source_file", "ingest_time").distinct())
