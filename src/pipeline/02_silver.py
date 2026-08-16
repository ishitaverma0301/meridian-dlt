# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC Two streaming tables fed by a shared view: Bronze →
# MAGIC `silver_flights_scored` (view) → `silver_flights_clean` /
# MAGIC `silver_flights_rejected`.
# MAGIC
# MAGIC Bronze is append-only and everything Silver does is row-wise and
# MAGIC stateless — compute `rejection_reason` from the 15 DQ rules, then split
# MAGIC on it. So every row is touched exactly once, ever. As materialized views
# MAGIC these re-derived the whole history on every refresh, which rewrote the
# MAGIC entire Silver table and forced all eight Gold tables to fully recompute
# MAGIC on every run. Streaming is what makes the incremental load real.
# MAGIC
# MAGIC `concat_ws` returns `""` (not `NULL`) when every rule is false for a
# MAGIC row — easy to miss, and consequential: skip coalescing that empty string
# MAGIC back to NULL and `rejection_reason` is never actually NULL, so
# MAGIC `IS NULL` / `IS NOT NULL` splits stop working — every row looks
# MAGIC rejected. `F.nullif(..., "")` is what restores it.
# MAGIC
# MAGIC ## Reprocessing a month — manual, and the order matters
# MAGIC
# MAGIC Reprocessing overwrites the file in `raw_landing`; Auto Loader
# MAGIC (`allowOverwrites=true`) re-reads it and **appends** a second copy of
# MAGIC that month to Bronze with a newer `ingested_at`. Nothing here dedupes
# MAGIC that — the old materialized-view Silver did, via a `MAX(ingested_at)`
# MAGIC self-join, and that join is exactly what made incremental load
# MAGIC impossible. So the reconciliation is now a manual step:
# MAGIC
# MAGIC 1. Run acquisition with `reprocess=True`, then run the pipeline.
# MAGIC 2. Find the two ingestion batches for the affected month:
# MAGIC    `SELECT ingest_year, ingest_month, ingested_at, COUNT(*)
# MAGIC     FROM meridian.bronze.bronze_flights_raw
# MAGIC     WHERE ingest_year = 'YYYY' AND ingest_month = 'MM'
# MAGIC     GROUP BY 1, 2, 3`
# MAGIC 3. `DELETE FROM meridian.bronze.bronze_flights_raw` for the **older**
# MAGIC    `ingested_at`. DML on a UC streaming table works from a SQL
# MAGIC    warehouse; use a literal timestamp, since Delta rejects referencing
# MAGIC    the target table in a DELETE subquery.
# MAGIC 4. Full refresh **Silver only** (Refresh selection → both tables).
# MAGIC
# MAGIC Step 3 is not optional. A Silver full refresh alone re-reads Bronze,
# MAGIC which still holds both copies, and reproduces the duplication. Bronze
# MAGIC is never full-refreshed, so `pipelines.reset.allowed: "false"` stays in
# MAGIC place and no CSVs are re-parsed.
# MAGIC
# MAGIC If reprocessing ever becomes routine rather than exceptional, replace
# MAGIC all of the above with `create_auto_cdc_flow` (SCD 1, keyed on the
# MAGIC natural flight key, sequenced by `ingested_at`) — a reprocessed row
# MAGIC then MERGEs over its predecessor automatically and none of these four
# MAGIC steps are needed.
# MAGIC
# MAGIC Unchanged from the first draft: published to `meridian.silver` via
# MAGIC fully-qualified names, and columns lowercased on the way in from Bronze
# MAGIC (the raw BTS header casing is inconsistent, and the DQ rules and Gold SQL
# MAGIC are written against lowercase columns). That belongs here, not in Bronze
# MAGIC — Bronze stays a faithful mirror of the source file.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

DQ_RULES = {
    "DQ01: Null flight date": "ingest_year IS NULL OR Month IS NULL OR DayofMonth IS NULL",
    "DQ02: Null airline": "Reporting_Airline IS NULL",
    "DQ03: Null route": "Origin IS NULL OR Dest IS NULL",
    "DQ04: Null distance": "Distance IS NULL",
    "DQ05: Invalid month": "Month IS NOT NULL AND (Month < 1 OR Month > 12)",
    "DQ06: Invalid day": "DayofMonth IS NOT NULL AND (DayofMonth < 1 OR DayofMonth > 31)",
    "DQ07: Non-positive distance": "Distance IS NOT NULL AND Distance <= 0",
    "DQ08: DepDelay out of range": "DepDelay IS NOT NULL AND (DepDelay < -60 OR DepDelay > 1440)",
    "DQ09: ArrDelay out of range": "ArrDelay IS NOT NULL AND (ArrDelay < -60 OR ArrDelay > 1440)",
    "DQ10: Invalid time": "(DepTime IS NOT NULL AND (DepTime < 0 OR DepTime > 2400)) OR (ArrTime IS NOT NULL AND (ArrTime < 0 OR ArrTime > 2400))",
    "DQ11: Cancelled with arrival time": "Cancelled = 1 AND ArrTime IS NOT NULL",
    "DQ12: Cancelled with arrival delay": "Cancelled = 1 AND ArrDelay IS NOT NULL",
    "DQ13: Operated flight missing deptime": "Cancelled = 0 AND Diverted = 0 AND DepTime IS NULL",
    "DQ14: Airtime exceeds elapsed time": "AirTime IS NOT NULL AND ActualElapsedTime IS NOT NULL AND AirTime > ActualElapsedTime",
    "DQ15: Origin equals destination": "Origin IS NOT NULL AND Origin = Dest",
}

_reason_expr = F.nullif(
    F.concat_ws(
        " | ",
        *[F.when(F.expr(cond), F.lit(name)) for name, cond in DQ_RULES.items()]
    ),
    F.lit(""),
)

def _lowercase_columns(df):
    return df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

@dp.view
def silver_flights_scored():
    """Bronze with columns lowercased and rejection_reason computed. A view,
    not a table: it is a pure pass-through, and `clean` and `rejected` are a
    total, disjoint split of it — materializing it would store every row twice
    and add a flow to every run for nothing that can't be reconstructed by
    union."""
    bronze = _lowercase_columns(spark.readStream.table("meridian.bronze.bronze_flights_raw"))
    return bronze.withColumn("rejection_reason", _reason_expr)

# COMMAND ----------

@dp.table(
    name="meridian.silver.silver_flights_clean",
    comment="Rows passing all DQ rules — this is what Gold reads from.",
    table_properties={"quality": "silver"},
)
def silver_flights_clean():
    return spark.readStream.table("silver_flights_scored").where(
        F.col("rejection_reason").isNull()
    )

# COMMAND ----------

@dp.table(
    name="meridian.silver.silver_flights_rejected",
    comment="Rows failing one or more DQ rules — persisted, not dropped, for steward audit.",
    table_properties={"quality": "silver"},
)
def silver_flights_rejected():
    return spark.readStream.table("silver_flights_scored").where(
        F.col("rejection_reason").isNotNull()
    )
