<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion-DAA520?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Lakeflow-Declarative_Pipelines-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Unity_Catalog-Governance-1B5A82?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta_Lake-Storage-00ADD8?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Lakeflow_Jobs-Orchestration-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Asset_Bundles-IaC-1B5A82?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white" />
</p>

---

## Meridian: Flight Data Lakehouse & Data Quality Framework

A production-grade lakehouse pipeline that ingests, validates, and transforms **US domestic flight data** from the Bureau of Transportation Statistics through a **Bronze → Silver → Gold** medallion architecture, built entirely on **Lakeflow Declarative Pipelines** and governed end-to-end by **Unity Catalog**. Designed for unattended monthly operation: idempotent by construction, with dependency-driven recomputation replacing hand-rolled state tracking wherever the platform can do the job natively.

---

### Project Highlights

- **State-free acquisition** — the ingestion task derives "what's already landed" by listing the raw Volume itself on every run, rather than tracking a separate cursor. Nothing to desynchronize, nothing for two overlapping runs to race on.
- **Publish-lag aware by default** — the incremental window never reaches for months the source can't possibly have published yet, based on its observed release cadence. No wasted attempts, no noisy failure log for a condition that was never actually a failure.
- **Non-destructive data quality** — all 15 validation rules run in a single pass, producing one `rejection_reason` column per row. Failing rows are persisted for audit, never silently dropped; only the clean, deduplicated set feeds Gold.
- **Reprocessing that's actually correct** — a corrected month re-lands, re-ingests, and reconciles down to the latest version automatically, with no risk of silently keeping stale data (the exact class of bug a naive Auto Loader setup falls into).
- **Externalized Gold SQL** — all 8 aggregate tables are defined as plain `.sql` files in a governed Volume, loaded and registered dynamically by a single orchestrating script. Editing an aggregation's logic means editing one SQL file — no redeploy, no touching pipeline code.
- **Dependency-driven, not manually orchestrated** — cross-table dependencies (a trend table built on top of a monthly KPI table, for instance) are discovered automatically from the query itself. No manual "has anything changed" bookkeeping, no separate recompute pass.
- **Fully governed namespace** — every object, from the landing Volume to the final Gold table, lives under one three-level Unity Catalog hierarchy, with bronze/silver/gold/ops each isolated in their own schema.
- **Infrastructure as code** — the full stack (catalog, schemas, volumes, pipeline, orchestration job) is defined declaratively and deployable as a single unit.

---

### Architecture

```
┌─────────────────┐     ┌──────────────────────────────────────────────┐
│   Acquisition    │     │              Lakeflow Pipeline                │
│  (scheduled job) │     │                                                │
│                  │     │   Bronze          Silver            Gold      │
│  Source ZIPs ────┼────▶│  streaming   ──▶  15 DQ rules  ──▶  8 tables  │
│  → raw Volume    │     │  table via        scored in a        + trend  │
│                  │     │  Auto Loader       shared view       (SQL,    │
│                  │     │                    → clean /         Volume- │
│                  │     │                      rejected        sourced)│
└─────────────────┘     └──────────────────────────────────────────────┘
        │                                    │
        └──────────── same Job, sequenced by task dependency ───────────┘
```

---

### Execution Model

**Acquisition** resolves its own run window on every trigger: it lists the landing Volume to determine the latest month already present, then walks forward to a fixed lag behind the current date — the empirically observed gap between a month ending and the source actually publishing it. A separate reprocess mode accepts an explicit month range and bypasses the "already landed" check entirely, so a corrected source file is always re-pulled on request.

**Bronze** is a single Auto Loader streaming table with overwrite detection enabled, so a reprocessed file is picked up rather than silently skipped. Because streaming tables are append-only, a reprocessed month temporarily exists as two versions side by side — this is expected, and resolved one layer down.

**Silver** computes a single `rejection_reason` expression across all 15 rules in one pass. That computation lives in a shared view, `silver_flights_scored`, which two streaming tables then filter into `silver_flights_clean` and `silver_flights_rejected` — scoring happens once, not once per output. Deduplication is deliberately *not* done here: a `MAX(ingested_at)` self-join is what forced a full recompute on every run and made incremental loading impossible, so the two-versions-of-one-month case from Bronze is instead reconciled by an explicit reprocessing procedure (documented in `silver_dqcheck.py`).

**Gold** is dependency-resolved, not manually sequenced: each of the 8 tables is defined as an externalized SQL file, and any table that reads from another Gold table (a trend view built on a KPI table, for example) has that dependency discovered automatically when the pipeline's graph is resolved — no explicit ordering, no separate "recompute derived tables" step.

**Failure semantics.** A genuine processing failure and "the source hasn't published this month yet" are classified separately at acquisition time — only the former is treated as an actionable failure with alerting; the latter is expected, logged, and resolves itself on the next scheduled run with no intervention.

---

### Data Quality Rule Catalog

All 15 rules execute in a single Spark SQL pass; a row can report multiple violations at once, joined into one `rejection_reason` string. Failing rows are persisted, never dropped.

| Rule | Check | Severity |
|------|-------|----------|
| 01–03 | Missing flight date, airline, or route | Critical |
| 04–06 | Missing or out-of-bounds distance, month, or day | Critical / Major |
| 07–10 | Implausible distance, delay, or time-of-day values | Major |
| 11–14 | Business-invariant violations (e.g. a cancelled flight can't have an arrival time; airtime can't exceed total elapsed time) | Logical |
| 15 | Degenerate same-airport route | Logical |

---

### Gold Layer Output Tables

Eight aggregate tables plus one cross-table trend view, each an independently versioned SQL artifact.

| Table | Description | Grain |
|-------|-------------|-------|
| `airline_monthly_kpi` | On-time %, cancellation rate, average delays per airline | airline × month |
| `route_monthly_kpi` | Route-level performance per carrier | route × airline × month |
| `route_summary` | Aggregate route traffic and performance | route × month |
| `airport_daily_ops` | Daily departure / arrival counts per airport | airport × direction × day |
| `delay_cause_analysis` | Delay breakdown by cause category per airline | airline × month |
| `time_block_performance` | Performance by departure hour block | airline × time block × month |
| `cancellation_analysis` | Cancellation reasons by airline | airline × reason × month |
| `monthly_trend` | Month-over-month + year-over-year trend, ranked | airline × month, cross-year |

---

### Key Features

| Feature | Description |
|---------|-------------|
| **Medallion Architecture** | Bronze (raw) → Silver (validated) → Gold (aggregated), each in its own governed schema |
| **Declarative Incremental Processing** | Auto Loader ingestion and materialized-view recomputation, no hand-written checkpoint or watermark logic |
| **15 Data Quality Rules** | Single-pass evaluation, non-destructive rejection, full audit trail |
| **State-Free Idempotent Acquisition** | "What's done" is derived from the landing Volume's actual contents, not a separately tracked cursor |
| **Externalized SQL Artifacts** | 8 Gold queries stored as versioned files in a governed Volume, loaded at runtime |
| **Dependency-Resolved DAG** | Cross-table dependencies within Gold are discovered automatically, not manually sequenced |
| **Scheduled Multi-Task Orchestration** | One job, dependency-ordered tasks, guarded against overlapping runs |
| **Infrastructure as Code** | Catalog, schemas, volumes, pipeline, and job all defined declaratively |

---

### Project Structure

```
meridian/
├── databricks.yml                       # Infrastructure as code: catalog, schemas,
│                                        #   volumes, pipeline, orchestration job
├── src/
│   ├── setup/
│   │   └── schema_creation.py           # Unity Catalog DDL: catalog, schemas, volumes
│   ├── acquisition/
│   │   └── fetch_bts_months.py          # Scheduled task: source → raw Volume
│   ├── pipeline/
│   │   ├── bronze_ingestion.py          # Bronze: Auto Loader streaming table
│   │   ├── silver_dqcheck.py            # Silver: 15 DQ rules → clean/rejected split
│   │   ├── gold_transformation.py       # Gold: loads & registers 8 tables from SQL artifacts
│   │   └── sql_artifacts/               # The 8 externalized SQL artifacts themselves
│   │       ├── airline_monthly_kpi.sql
│   │       ├── route_monthly_kpi.sql
│   │       ├── route_summary.sql
│   │       ├── airport_daily_ops.sql
│   │       ├── delay_cause_analysis.sql
│   │       ├── time_block_performance.sql
│   │       ├── cancellation_analysis.sql
│   │       └── monthly_trend.sql
│   ├── ops/
│   │   └── run_summary.py               # Builds the per-run operational summary table
│   └── alerts/
│       ├── run_summary_alert.sql        # Query behind the run-summary SQL Alert
│       └── run_digest_alert.sql         # Query behind the digest SQL Alert
```

> Filenames match the notebook names in the Databricks workspace, so a Git folder
> clone maps 1:1 onto the workspace rather than creating a parallel set of copies.

---

### Data Source

[Bureau of Transportation Statistics (BTS)](https://www.transtats.bts.gov/) — US Domestic Flight On-Time Performance. Published monthly, roughly 500K+ rows per month.

---

### Tech Stack

| Layer | Technology |
|-------|-----------|
| Distributed Processing | PySpark (Spark SQL, DataFrames) |
| Ingestion | Python, scheduled task |
| Storage | Delta Lake — bronze / silver / gold, each in its own governed schema |
| Transformation | Lakeflow Declarative Pipelines |
| Orchestration | Lakeflow Jobs — scheduled, dependency-ordered, concurrency-guarded |
| Data Governance | Unity Catalog — three-level namespace, managed Volumes |
| Infrastructure | Declarative bundle definition (catalog, schemas, volumes, pipeline, job) |
| Data Format | Delta (Parquet + transaction log) |
