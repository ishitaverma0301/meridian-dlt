# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC Streaming table, Auto Loader off the `raw_landing` Volume. The pipeline
# MAGIC manages its own checkpoint/schema-inference state internally, so
# MAGIC there's no hand-specified checkpoint path to accidentally nest inside
# MAGIC the scanned source directory (`SCHEMA_PATH` below lives in a separate
# MAGIC `pipeline_internals` Volume, not under `raw_landing`).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/meridian/bronze/raw_landing"
SCHEMA_PATH = "/Volumes/meridian/bronze/pipeline_internals/flights_schema"

# COMMAND ----------

@dp.table(
    name="bronze_flights_raw",
    comment="Raw BTS On-Time Performance records, landed via Auto Loader from raw_landing.",
    table_properties={
        "quality": "bronze",
        # NOTE: temporarily "true" to allow a one-off Full Refresh that rebuilds
        # Bronze from raw_landing. Set back to "false" immediately afterwards —
        # normally this guard is what stops a full reset from wiping Bronze and
        # forcing Auto Loader to reprocess the entire Volume from scratch.
        "pipelines.reset.allowed": "true",
    },
)
@dp.expect_or_drop("valid_partition", "ingest_year IS NOT NULL AND ingest_month IS NOT NULL")
@dp.expect("has_flight_date", "FlightDate IS NOT NULL")
def bronze_flights_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("header", "true")
        .load(RAW_VOLUME_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingest_year", F.regexp_extract("source_file", r"year=(\d{4})", 1))
        .withColumn("ingest_month", F.regexp_extract("source_file", r"month=(\d{2})", 1))
        .withColumn("ingested_at", F.current_timestamp())
    )
