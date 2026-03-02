# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS multisales_catalog;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS multisales_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS multisales_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS multisales_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS multisales_catalog.bronze.raw_files;
