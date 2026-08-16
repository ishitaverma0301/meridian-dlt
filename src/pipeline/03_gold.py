# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold Layer
# MAGIC
# MAGIC The only pipeline source file for all 8 Gold tables. Each table's SQL
# MAGIC lives as a plain `.sql` file in the `sql_artifacts` Volume, not in this
# MAGIC script or as a separate registered pipeline source — this script just
# MAGIC loops over the list, reads each file's text, and registers a
# MAGIC materialized view from it.
# MAGIC
# MAGIC The whole point being that an
# MAGIC analyst can edit the aggregation logic without redeploying the job. Same
# MAGIC contract here: edit a `.sql` file in the Volume, re-run the pipeline,
# MAGIC done — nothing in this script changes.
# MAGIC
# MAGIC `monthly_trend` reads `FROM meridian.gold.airline_monthly_kpi` inside its
# MAGIC own `.sql` file. Lakeflow discovers that reference when it runs this
# MAGIC script during graph resolution and orders the DAG accordingly — computed
# MAGIC last, automatically, no separate cross-year recompute step required
# MAGIC (unlike the AWS version's `recompute_cross_year_tables()`).

# COMMAND ----------

from pyspark import pipelines as dp

SQL_ARTIFACTS_PATH = "/Volumes/meridian/gold/sql_artifacts"

GOLD_TABLES = [
    "airline_monthly_kpi",
    "route_monthly_kpi",
    "route_summary",
    "airport_daily_ops",
    "delay_cause_analysis",
    "time_block_performance",
    "cancellation_analysis",
    "monthly_trend",  # depends on airline_monthly_kpi — order in this list doesn't matter, Lakeflow resolves it from the SQL itself
]

# COMMAND ----------

def _load_sql(table_name):
    with open(f"{SQL_ARTIFACTS_PATH}/{table_name}.sql") as f:
        return f.read()

def _register_gold_table(table_name, sql_text):
    @dp.materialized_view(
        name=f"meridian.gold.{table_name}",
        comment=f"Gold table `{table_name}` — SQL sourced from {SQL_ARTIFACTS_PATH}/{table_name}.sql",
    )
    def gold_view():
        return spark.sql(sql_text)
    return gold_view

# COMMAND ----------

for _table_name in GOLD_TABLES:
    _register_gold_table(_table_name, _load_sql(_table_name))