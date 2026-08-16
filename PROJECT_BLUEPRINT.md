# Meridian — Project Blueprint

**Purpose of this file**: a self-contained handoff document. Read this and you should be able to continue the project without re-explaining anything. `README.md` in this same repo is the portfolio-facing description of the finished system — this file is the working-notes version: decisions, current status, gotchas, exact next steps.

---

## 1. What this project is

A flight-data lakehouse pipeline on Databricks — Bronze → Silver → Gold medallion architecture, built on Lakeflow Declarative Pipelines, ingesting Bureau of Transportation Statistics (BTS) monthly on-time-performance data. It is a **rebuild** of an existing AWS project (Glue + Step Functions + S3 + CloudFormation) onto native Databricks services — the goal is maximum genuine use of Databricks-native services (Unity Catalog, Auto Loader, Lakeflow Declarative Pipelines, Lakeflow Jobs, Databricks Asset Bundles) for portfolio/resume purposes, not a literal 1:1 port. Where Databricks' platform already solves something the AWS version had to hand-build (watermarks, atomic writes, cross-year recompute), we deliberately dropped the hand-built version rather than porting it.

**How this is being built**: entirely through the **Databricks workspace UI** (SQL editor, notebook import, pipeline/job UI) — no Databricks CLI, by explicit user preference. The one known exception: Databricks Asset Bundles (Phase 8, not started) require the CLI to actually deploy; that tension hasn't been resolved yet.

**Working style**: this is an explicit *learning* exercise, not just "get it built." Explain code, don't just hand over blocks. Verify Databricks API/syntax claims via search before giving live code (this has mattered — see Known Issues below). Prefer Import-a-file over manual retype for anything long, since retyping/copy-paste has caused real bugs twice.

**Source material** (for reference, not copied verbatim): the original AWS project lives at `/Users/ishitaverma/Downloads/aws-etl-pipeline_project/GitHub/` — `README.md`, `docs/data-quality-rules.md`, `docs/pipeline-operations.md`, `glue_jobs/{ingestion,dqcheck,transformation}.py`, `sql/gold/*.sql` (8 files), `orchestration/step_function.json`, `infrastructure/cloudformation.yaml`. All of this project's code and docs have been deliberately scrubbed of AWS references (explicit user instruction) — it stands on its own.

**This project's local copy**: `/Users/ishitaverma/ClaudeCode/meridian-dlt/` — mirrors what should exist in the Databricks workspace. Treat it as the source of truth for what the workspace *should* contain; if the two disagree, the workspace is what's actually running, but this repo is what to re-sync from.

---

## 2. Unity Catalog layout (already created)

```
meridian (catalog)
├── bronze (schema)
│   ├── raw_landing (Volume)          — raw BTS CSVs land here, year=YYYY/month=MM/
│   ├── pipeline_internals (Volume)   — Auto Loader schema-inference state (NOT nested in raw_landing)
│   └── bronze_flights_raw (table)    — Auto Loader streaming table
├── silver (schema)
│   ├── silver_flights_all (table)    — deduped + rejection_reason computed, internal staging
│   ├── silver_flights_rejected (table) — audit trail, non-destructive
│   └── silver_flights_clean (table)  — what Gold reads from
├── gold (schema)
│   ├── sql_artifacts (Volume)        — the 8 bare-SELECT .sql files, read at pipeline runtime
│   ├── airline_monthly_kpi (table)
│   ├── route_monthly_kpi (table)
│   ├── route_summary (table)
│   ├── airport_daily_ops (table)
│   ├── delay_cause_analysis (table)
│   ├── time_block_performance (table)
│   ├── cancellation_analysis (table)
│   └── monthly_trend (table)
└── ops (schema)
    └── ingestion_events (table)      — append-only acquisition log, observability only
```

Catalog was originally named `skyledger`, renamed to `meridian` early on (dropped and recreated cleanly, no migration needed).

---

## 3. Workspace layout (notebooks)

```
meridian/                                (workspace folder)
├── acquisition/
│   └── fetch_bts_months                 — Job task, NOT part of the Lakeflow pipeline
└── pipeline/
    ├── 01_bronze                        — pipeline source file 1
    ├── 02_silver                        — pipeline source file 2
    ├── 03_gold                          — pipeline source file 3 (only Gold file registered — see §5)
    └── gold/  (NOT a pipeline source location — these get uploaded to the sql_artifacts Volume, not attached to the pipeline)
```

Databricks' own default convention when creating a pipeline from a folder is a `transformations/` subfolder — noted for awareness, but we deliberately did NOT migrate to it since `01_bronze`/`02_silver` were already working under `meridian/pipeline/` and there was no benefit to switching.

The Lakeflow Pipeline resource is named `meridian_flights_pipeline`, destination catalog `meridian`, default schema `bronze` (Bronze uses the default; Silver and Gold override via fully-qualified table names in their own definitions — see §5).

---

## 4. Acquisition (`fetch_bts_months.py`) — design summary

- **No watermark table.** State ("what months are already landed") is derived by listing the `raw_landing` Volume on every run (`discover_landed_months()`), not tracked in a separate table. This was a deliberate fix: an earlier iteration used a `MERGE`-based watermark table and hit a real race-condition bug (two overlapping runs could regress the watermark) — the fix generalizes to "don't track state that can drift from reality, derive it."
- **`PUBLISH_LAG_MONTHS = 2`** — BTS publishes roughly monthly with an empirically observed ~6–7 week lag. The default incremental window never reaches for the most recent 1–2 months, avoiding wasted download attempts on data that can't exist yet.
- **Reprocess mode** (`reprocess=True` widget + `start_date`/`end_date`) bypasses the "already landed" check and force-redownloads, overwriting the existing file at the same path.
- **Three-way outcome per month**: `landed` / `not_published` (expected, self-heals next run — 404, tiny file, corrupt zip) / `failed` (real exception). Only `failed` raises `RuntimeError`.
- **Append-only event log** (`meridian.ops.ingestion_events`) for observability — critically, nothing reads this back to make decisions. That decoupling is what avoids rebuilding the watermark-race bug in a new shape.
- Status: **built, imported, backfilled successfully** (18 months, 2025-01 through 2026-06, zero failures).

---

## 5. Lakeflow Pipeline — design summary

Three source files register into one pipeline (`meridian_flights_pipeline`), Bronze → Silver → Gold, ~12 tables total.

**`01_bronze.py`** — one `@dp.table` (streaming table) via Auto Loader on `raw_landing`.
- `cloudFiles.allowOverwrites = true` — without this, a reprocessed/overwritten file is silently skipped (this was a real bug we hit and fixed).
- `cloudFiles.schemaLocation` points at the **separate** `pipeline_internals` Volume, not nested inside `raw_landing` — Auto Loader recursively scans whatever it's pointed at, so pipeline metadata can't live inside the scanned tree. (Also hit and fixed: `UC_VOLUME_NOT_FOUND` — the path segment after `/Volumes/<catalog>/<schema>/` must be a real registered Volume, not an arbitrary folder name.)
- `table_properties: {"pipelines.reset.allowed": "false"}` protects Bronze from a Full Refresh wipe.
- Adds `ingest_year`/`ingest_month` (regex-parsed from the file path) and `ingested_at` (current_timestamp at read time).

**`02_silver.py`** — three `@dp.materialized_view`s, published to `meridian.silver` via fully-qualified names (`name="meridian.silver.silver_flights_all"` etc. — **this** is the actual mechanism for a pipeline writing to multiple schemas; there is no separate `schema=` kwarg for table placement, that parameter means something else entirely — the table's own column schema).
- **`silver_flights_all`**: dedups Bronze to latest `ingested_at` per `(ingest_year, ingest_month)` — this is what reconciles the duplicate rows Bronze accumulates when a month gets reprocessed (Bronze is append-only by construction; reprocessing doesn't replace rows, it adds a second copy with a newer timestamp, and this view is what picks the winner). Also lowercases every column name here (`.toDF(*[c.lower() for c in df.columns])`) — not in Bronze, deliberately, since Bronze should stay a faithful unmodified mirror of the source. Then computes `rejection_reason` across all 16 DQ rules in a single pass.
- **`silver_flights_rejected`**: `WHERE rejection_reason IS NOT NULL` — persisted for audit, never dropped.
- **`silver_flights_clean`**: `WHERE rejection_reason IS NULL`, plus `dropDuplicates()` (DQ16, exact-row dedup). This is what Gold reads from.
- **Known bug, fixed**: `concat_ws` returns `""` (not `NULL`) when every rule is false for a row. Without wrapping in `F.nullif(..., "")`, `rejection_reason` was never actually `NULL`, so every row looked rejected — 100% rejection rate in testing. Fixed by wrapping the whole `concat_ws` expression in `nullif`.
- The 16 rules themselves live in a `DQ_RULES` dict (name → SQL condition string), categorized Critical (missing identifier) / Major (implausible value) / Logical (business-invariant violation) — full list in the file itself and in `README.md`.

**`03_gold.py`** — the **only** Gold pipeline source file. Loops over a list of 8 table names, reads each one's SQL as plain text from the `meridian.gold.sql_artifacts` Volume (not from the pipeline source code — these are runtime-loaded data artifacts, matching the "editable without redeploying" goal), and registers a `@dp.materialized_view` per table via `spark.sql(sql_text)`.
- `monthly_trend.sql` reads `FROM meridian.gold.airline_monthly_kpi` directly — Lakeflow discovers this as a pipeline-internal dependency during graph resolution and computes `airline_monthly_kpi` first automatically. No manual cross-table sequencing needed.
- Has: per-table print logging, a clear error if an artifact file is missing, and a guard rejecting a `.sql` file that looks like it still has a `CREATE MATERIALIZED VIEW` wrapper (catches the "someone re-uploaded the wrong version" mistake class).
- The 8 `.sql` files themselves (`src/pipeline/gold/*.sql` locally) are **bare `SELECT` statements**, no DDL wrapper — the wrapping happens in `03_gold.py`, not the artifact.
- Gold column names: verified clean (proper snake_case) across all 8 tables — every computed/selected output column is explicitly aliased in its SQL file, so Silver's smashed-lowercase pass-through columns (e.g. `deptimeblk`, `cancellationcode`) never leak into Gold's actual output unaliased. One exception found and fixed: `airport_daily_ops` was outputting a raw `dayofmonth` column instead of `day_of_month` — fixed (source reference inside `CAST()` correctly still reads Silver's actual `dayofmonth` column; only the alias changed to `day_of_month`).
- Materialized views were chosen over hand-written imperative Delta writes specifically for: automatic dependency-DAG resolution (the `monthly_trend` case above), a shot at incremental refresh for `GROUP BY`-shaped queries, and pipeline-managed consistency/retry handling. Known limitation, discussed and accepted: `monthly_trend`'s window functions (`LAG`, `RANK`) likely force a full recompute every run rather than incremental — acceptable because Gold tables are aggregated to airline×month grain and stay small (thousands of rows) regardless of how much raw data accumulates upstream. The layer that actually has a real "gets slower as data grows" risk is **Silver**, not Gold (full row-grain dedup join against all of Bronze on every refresh) — not a problem yet at 18 months of data, but the lever if it ever becomes one is bounding Silver's dedup computation to a trailing window of recent months rather than all of Bronze.

---

## 6. Orchestration (Lakeflow Job) — in progress

Design finalized, build steps given, **not yet confirmed built/tested by the user as of this writing**:

- Job name: `meridian_flights_ingestion`
- Task 1 `fetch_bts_months` (Notebook task) → Task 2 `refresh_pipeline` (Pipeline task), **Depends on** Task 1 — this dependency is the entire "run acquisition, then trigger the pipeline" mechanism, no custom polling/triggering code.
- Schedule: monthly vs. weekly discussed, leaning weekly (acquisition is idempotent, weekly costs almost nothing and catches a newly-published month faster) — final choice left to user.
- Max concurrent runs = 1 (race-condition guard at the orchestration layer).
- Failure email notification.
- **Explicitly decided against**: merging acquisition into the Lakeflow Pipeline itself as a `@dp.table`. Reasoning: acquisition has real side effects (HTTP calls, file writes) and doesn't produce a natural table shape; Lakeflow may re-invoke a table function multiple times during graph resolution/retries, which would be wrong for a function that downloads files. A Job with two tasks already presents as "one thing" (one schedule, one run, one status) — the Job/Pipeline split is an implementation detail, not something that should be visible as "two systems."
- **Not yet decided**: whether to add a third task (`notify_summary`) posting actual row-count/rejection stats via email or Slack. Recommendation given: routine volume stats belong on a dashboard (persistent, browsable) rather than an email; reserve email/Slack for an actionable SQL Alert on rejection-rate threshold. User hasn't chosen yet.

---

## 7. What's built vs. what's left

| Phase | Status |
|---|---|
| Databricks workspace access (UI only, no CLI) | ✅ |
| Unity Catalog foundation (catalog/schemas/volumes) | ✅ |
| Acquisition notebook, built and backfilled | ✅ |
| Bronze layer | ✅ |
| Silver layer (16 DQ rules, dedup, clean/rejected split) | ✅ (bug-fixed) |
| Gold layer (8 tables + monthly_trend, single orchestrator script) | ✅ (bug-fixed) |
| Lakeview dashboard on Gold | ❌ not started |
| Lakeflow Job (orchestration) | 🔶 designed, build steps given, not confirmed done |
| Databricks Asset Bundle (IaC) | ❌ not started — blocked on the CLI-vs-UI tension, unresolved |

`README.md` (portfolio-facing) and this file are both current as of the Gold column-naming fix.

---

## 8. Known gotchas worth remembering

- **Files have disappeared from the local `meridian-dlt/` repo unexplained, twice** (`fetch_bts_months.py` and `03_gold.py` both vanished between being written and later being read). Cause unknown. If a file that should exist doesn't, recreate it from this blueprint's description rather than assuming it was never made — check `find meridian-dlt -type f` at the start of a session.
- **Copy-paste/retyping across chat has caused real bugs twice**: the `F.nullif(...)` fix got mangled twice when typed manually, and a blind `replace_all` on `"dayofmonth"` broke a query by also renaming a source-column reference that needed to stay as-is. Prefer giving complete code blocks for one clean copy, or Import-the-file for anything long, over incremental manual edits.
- **Lakeflow terminology traps**: the `schema=` argument on `@dp.table`/`@dp.materialized_view` is for the table's *column* schema, not which Unity Catalog schema it publishes to (that's controlled by using a fully-qualified `name=`). Verified via search, not assumed — worth re-verifying if the API changes.
- The Databricks CLI is explicitly off the table per user preference, except Asset Bundles (Phase 8) fundamentally need it to deploy — this hasn't been resolved and will need a decision when we get there.
