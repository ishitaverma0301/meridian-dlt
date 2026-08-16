# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS meridian
# MAGIC COMMENT 'Meridian flights lakehouse';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS meridian.bronze COMMENT 'Raw landing volume + bronze streaming table';
# MAGIC CREATE SCHEMA IF NOT EXISTS meridian.silver COMMENT 'DQ-checked clean and rejected flight records';
# MAGIC CREATE SCHEMA IF NOT EXISTS meridian.gold   COMMENT 'Aggregated analytics tables';
# MAGIC CREATE SCHEMA IF NOT EXISTS meridian.ops    COMMENT 'Pipeline operational metadata';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS meridian.bronze.raw_landing
# MAGIC COMMENT 'Raw BTS monthly CSVs, landed by the fetch_bts_months job task';
# MAGIC
# MAGIC SHOW SCHEMAS IN meridian;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS meridian.bronze.pipeline_internals
# MAGIC COMMENT 'Auto Loader schema-inference state — kept out of raw_landing so the scanned source tree and pipeline metadata never mix.';