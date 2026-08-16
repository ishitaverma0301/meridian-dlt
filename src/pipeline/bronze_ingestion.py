# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/meridian/bronze/raw_landing"
# Deliberately a sibling of raw_landing, not nested inside it — Auto Loader
# scans RAW_VOLUME_PATH recursively, so schema-inference state must live
# outside the tree it's scanning.
SCHEMA_PATH = "/Volumes/meridian/bronze/pipeline_internals/flights_schema"

# COMMAND ----------

@dp.table(
    name="bronze_flights_raw",
    comment="Raw BTS On-Time Performance records, landed via Auto Loader from raw_landing.",
    table_properties={
        "quality": "bronze",
        # A full pipeline reset would otherwise wipe bronze and force
        # Auto Loader to reprocess the entire Volume from scratch.
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