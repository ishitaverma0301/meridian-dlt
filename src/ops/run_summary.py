# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
# MAGIC %md
# MAGIC # Run Summary — observability layer
# MAGIC
# MAGIC Assembles an exhaustive per-run report into `meridian.ops.run_summary`,
# MAGIC which the `meridian_run_summary` SQL Alert then emails. Runs as the last
# MAGIC task of `meridian_flights_ingestion`, depending on `refresh_pipeline`, so
# MAGIC it only fires after a successful pipeline update.
# MAGIC
# MAGIC **Why a notebook and not one big alert query.** A SQL Alert is a single
# MAGIC query, and its email template can only iterate that query's rows. Every
# MAGIC fact below could technically be reached in SQL — `event_log()` is a
# MAGIC table-valued function — but as one union it would be unmaintainable, and
# MAGIC the template caps at 100 rows. Assembling here and persisting to a table
# MAGIC makes the alert trivial, and gives the report history: rejection rate per
# MAGIC run becomes trendable, and the dashboard can read the same table.
# MAGIC
# MAGIC **Output shape** is tall — `(section, seq, severity, metric, value,
# MAGIC detail)` — so the email renders as one flat table regardless of how many
# MAGIC sections fire, and new sections never require a template change.
# MAGIC
# MAGIC **Every section is independently guarded.** A section that raises emits a
# MAGIC `SECTION FAILED` row at WARN and the rest of the report still builds. A
# MAGIC monitoring tool that dies when one of its probes dies is worse than no
# MAGIC monitoring tool, because it fails silently at exactly the moment
# MAGIC something is wrong.

# COMMAND ----------

from datetime import datetime, timezone, date
import traceback

CATALOG = "meridian"
SUMMARY_TABLE = f"{CATALOG}.ops.run_summary"
EVENTS_TABLE = f"{CATALOG}.ops.ingestion_events"

# event_log() is keyed to the pipeline that owns the table, so any table in the
# pipeline returns the whole pipeline's log. Gold is used as the anchor because
# it is the last thing to change.
EVENT_LOG_ANCHOR = f"{CATALOG}.gold.airline_monthly_kpi"

GOLD_TABLES = [
    "airline_monthly_kpi", "route_monthly_kpi", "route_summary",
    "airport_daily_ops", "delay_cause_analysis", "time_block_performance",
    "cancellation_analysis", "monthly_trend",
]

# Mirrors fetch_bts_months.py — BTS publishes on a ~6–7 week lag, so the most
# recent 2 months are not expected to exist yet and their absence is not a gap.
PUBLISH_LAG_MONTHS = 2

REJECTION_PCT_FAIL = 5.0        # hard ceiling on rejected rows for a month
VOLUME_DEVIATION_WARN = 0.30    # vs trailing average
VOLUME_DEVIATION_FAIL = 0.40

# DQ rule categories, per the data-quality catalog.
RULE_CATEGORY = {
    **{f"DQ{n:02d}": "Critical" for n in range(1, 5)},    # missing identifier
    **{f"DQ{n:02d}": "Major" for n in range(5, 11)},      # implausible value
    **{f"DQ{n:02d}": "Logical" for n in range(11, 16)},   # invariant violation
}

ERROR, WARN, INFO = "ERROR", "WARN", "INFO"
RANK = {INFO: 1, WARN: 2, ERROR: 3}

# COMMAND ----------

rows = []

def emit(section, severity, metric, value, detail=""):
    rows.append({
        "section": section,
        "severity": severity,
        "metric": str(metric),
        "value": "" if value is None else str(value),
        "detail": "" if detail is None else str(detail),
    })

def section(name):
    """Decorator: run a section, and turn any failure into a WARN row rather
    than letting it abort the whole report."""
    def wrap(fn):
        def run():
            try:
                fn()
            except Exception as e:
                emit(name, WARN, "SECTION FAILED", type(e).__name__, str(e)[:400])
                print(f"[{name}] failed: {e}")
                traceback.print_exc()
        run.__name__ = fn.__name__
        return run
    return wrap

def q(sql):
    return spark.sql(sql).collect()

def one(sql, default=None):
    r = q(sql)
    return r[0][0] if r and r[0][0] is not None else default

def ym_int(y, m):
    return int(y) * 12 + int(m)

def ym_label(y, m):
    return f"{int(y):04d}-{int(m):02d}"

def pct(num, den):
    return round(100.0 * num / den, 2) if den else 0.0

# COMMAND ----------

# Identify the run. The pipeline update is the better anchor (it is what
# actually produced the data); the acquisition run_id is the fallback.
LATEST_UPDATE = one(f"""
    SELECT origin.update_id FROM event_log(TABLE({EVENT_LOG_ANCHOR}))
    ORDER BY timestamp DESC LIMIT 1
""")
LATEST_ACQ_RUN = one(f"""
    SELECT run_id FROM {EVENTS_TABLE} ORDER BY event_time DESC LIMIT 1
""")
RUN_ID = LATEST_UPDATE or LATEST_ACQ_RUN or datetime.now(timezone.utc).isoformat()
GENERATED_AT = datetime.now(timezone.utc)

print(f"pipeline update : {LATEST_UPDATE}")
print(f"acquisition run : {LATEST_ACQ_RUN}")

# COMMAND ----------

# MAGIC %md ## Section 2 — Acquisition

@section("2. Acquisition")
def acquisition():
    if not LATEST_ACQ_RUN:
        emit("2. Acquisition", INFO, "No acquisition history", "—",
             f"{EVENTS_TABLE} is empty")
        return

    recs = q(f"""
        SELECT year, month, status, detail FROM {EVENTS_TABLE}
        WHERE run_id = '{LATEST_ACQ_RUN}' ORDER BY year, month
    """)
    counts = {"landed": 0, "not_published": 0, "failed": 0}
    for r in recs:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    emit("2. Acquisition", ERROR if counts["failed"] else INFO,
         "Months attempted", len(recs), f"acquisition run {LATEST_ACQ_RUN}")
    emit("2. Acquisition", INFO, "Landed", counts["landed"])
    emit("2. Acquisition", WARN if counts["not_published"] else INFO,
         "Not published", counts["not_published"],
         "expected for recent months — self-heals next run")
    emit("2. Acquisition", ERROR if counts["failed"] else INFO,
         "Failed", counts["failed"])

    # Only itemise months that need attention, plus anything newly landed.
    for r in recs:
        if r["status"] == "failed":
            emit("2. Acquisition", ERROR, ym_label(r["year"], r["month"]),
                 "failed", r["detail"])
        elif r["status"] == "landed":
            emit("2. Acquisition", INFO, ym_label(r["year"], r["month"]),
                 "landed", r["detail"])

acquisition()

# COMMAND ----------

# MAGIC %md ## Section 3 — Coverage and freshness

@section("3. Coverage")
def coverage():
    months = q(f"""
        SELECT CAST(ingest_year AS INT) AS y, CAST(ingest_month AS INT) AS m,
               COUNT(*) AS n
        FROM {CATALOG}.bronze.bronze_flights_raw
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    if not months:
        emit("3. Coverage", ERROR, "Bronze is empty", 0, "no months ingested")
        return

    present = {ym_int(r["y"], r["m"]): r["n"] for r in months}
    lo, hi = min(present), max(present)
    emit("3. Coverage", INFO, "Month range",
         f"{ym_label(lo // 12, lo % 12 or 12)} → {ym_label(hi // 12, hi % 12 or 12)}",
         f"{len(present)} months present")

    gaps = [k for k in range(lo, hi + 1) if k not in present]
    emit("3. Coverage", WARN if gaps else INFO, "Gaps inside range", len(gaps),
         ", ".join(ym_label(k // 12, k % 12 or 12) for k in gaps[:12]) or "none")

    empties = [k for k, n in present.items() if n == 0]
    if empties:
        emit("3. Coverage", ERROR, "Months with zero rows", len(empties),
             ", ".join(ym_label(k // 12, k % 12 or 12) for k in empties[:12]))

    # Freshness: BTS cannot have published the last PUBLISH_LAG_MONTHS months.
    today = date.today()
    expected = ym_int(today.year, today.month) - PUBLISH_LAG_MONTHS
    behind = expected - hi
    emit("3. Coverage", WARN if behind > 0 else INFO, "Freshness",
         "up to date" if behind <= 0 else f"{behind} month(s) behind",
         f"latest present {ym_label(hi // 12, hi % 12 or 12)}, "
         f"expected {ym_label(expected // 12, expected % 12 or 12)}")

coverage()

# COMMAND ----------

# MAGIC %md ## Section 4 — Reconciliation funnel
# MAGIC
# MAGIC Rows dropped by Bronze's `expect_or_drop("valid_partition")` never reach
# MAGIC Silver, so they appear in neither `clean` nor `rejected`. Nothing else in
# MAGIC the project reports them. If Bronze and Silver totals disagree by
# MAGIC anything other than that drop count, something is wrong upstream and no
# MAGIC other metric will say so.

@section("4. Reconciliation")
def reconciliation():
    bronze_n = one(f"SELECT COUNT(*) FROM {CATALOG}.bronze.bronze_flights_raw", 0)
    clean_n = one(f"SELECT COUNT(*) FROM {CATALOG}.silver.silver_flights_clean", 0)
    rej_n = one(f"SELECT COUNT(*) FROM {CATALOG}.silver.silver_flights_rejected", 0)
    silver_n = clean_n + rej_n
    delta = bronze_n - silver_n

    emit("4. Reconciliation", INFO, "Bronze rows", f"{bronze_n:,}")
    emit("4. Reconciliation", INFO, "Silver clean", f"{clean_n:,}")
    emit("4. Reconciliation", INFO, "Silver rejected", f"{rej_n:,}")
    emit("4. Reconciliation", ERROR if delta != 0 else INFO,
         "Bronze − Silver", f"{delta:,}",
         "must be 0 — Silver is a total, disjoint split of Bronze"
         if delta else "reconciles")

    # Bronze expectation results, from the pipeline event log.
    try:
        exp = q(f"""
            SELECT e.name AS name,
                   SUM(TRY_CAST(e.passed_records AS BIGINT)) AS passed,
                   SUM(TRY_CAST(e.failed_records AS BIGINT)) AS failed
            FROM event_log(TABLE({EVENT_LOG_ANCHOR}))
            LATERAL VIEW EXPLODE(FROM_JSON(
                details:flow_progress:data_quality:expectations,
                'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
            )) t AS e
            WHERE event_type = 'flow_progress'
              AND origin.update_id = '{LATEST_UPDATE}'
            GROUP BY e.name
        """)
        if not exp:
            emit("4. Reconciliation", INFO, "Bronze expectations",
                 "none reported", "no expectation metrics in this update")
        for r in exp:
            failed = r["failed"] or 0
            # valid_partition drops rows; has_flight_date only warns.
            drops = r["name"] == "valid_partition"
            emit("4. Reconciliation", WARN if failed else INFO,
                 f"Expectation: {r['name']}", f"{failed:,} failed",
                 "rows DROPPED before Silver" if drops and failed
                 else "retained, warning only" if failed else "clean")
    except Exception as e:
        emit("4. Reconciliation", WARN, "Bronze expectations",
             "unavailable", f"{type(e).__name__}: {str(e)[:200]}")

reconciliation()

# COMMAND ----------

# MAGIC %md ## Section 5 — Data quality

@section("5. Data quality")
def data_quality():
    latest = q(f"""
        SELECT CAST(ingest_year AS INT) AS y, CAST(ingest_month AS INT) AS m
        FROM {CATALOG}.silver.silver_flights_clean
        ORDER BY ingested_at DESC LIMIT 1
    """)
    if not latest:
        emit("5. Data quality", ERROR, "Silver is empty", 0)
        return
    y, m = latest[0]["y"], latest[0]["m"]
    scope = f"CAST(ingest_year AS INT) = {y} AND CAST(ingest_month AS INT) = {m}"

    clean_n = one(f"SELECT COUNT(*) FROM {CATALOG}.silver.silver_flights_clean WHERE {scope}", 0)
    rej_n = one(f"SELECT COUNT(*) FROM {CATALOG}.silver.silver_flights_rejected WHERE {scope}", 0)
    total = clean_n + rej_n
    rate = pct(rej_n, total)

    emit("5. Data quality", INFO, "Month evaluated", ym_label(y, m))
    emit("5. Data quality", INFO, "Rows evaluated", f"{total:,}")
    emit("5. Data quality", ERROR if rate > REJECTION_PCT_FAIL else INFO,
         "Rejection rate", f"{rate}%",
         f"threshold {REJECTION_PCT_FAIL}% · {rej_n:,} of {total:,} rows")

    # Trailing comparison — a rate that is fine in absolute terms but double
    # last month's is the more interesting signal.
    hist = q(f"""
        SELECT CAST(ingest_year AS INT) * 12 + CAST(ingest_month AS INT) AS ym,
               COUNT(*) AS n
        FROM {CATALOG}.silver.silver_flights_rejected GROUP BY 1
    """)
    tot = q(f"""
        SELECT CAST(ingest_year AS INT) * 12 + CAST(ingest_month AS INT) AS ym,
               COUNT(*) AS n
        FROM {CATALOG}.silver.silver_flights_clean GROUP BY 1
    """)
    rej_by = {r["ym"]: r["n"] for r in hist}
    tot_by = {r["ym"]: r["n"] for r in tot}
    this_ym = ym_int(y, m)
    prior = [pct(rej_by.get(k, 0), rej_by.get(k, 0) + tot_by.get(k, 0))
             for k in sorted(tot_by) if k < this_ym][-6:]
    if prior:
        avg = round(sum(prior) / len(prior), 2)
        drift = round(rate - avg, 2)
        emit("5. Data quality", WARN if drift > 1.0 else INFO,
             "vs trailing average", f"{drift:+}pp",
             f"this month {rate}% · previous {len(prior)}-month avg {avg}%")

    # Per-rule counts. A row can violate several rules, so these sum to more
    # than the rejected-row count.
    per_rule = q(f"""
        SELECT TRIM(reason) AS rule, COUNT(*) AS n
        FROM {CATALOG}.silver.silver_flights_rejected
        LATERAL VIEW EXPLODE(SPLIT(rejection_reason, '\\\\|')) t AS reason
        WHERE {scope}
        GROUP BY TRIM(reason) ORDER BY n DESC
    """)
    fired = set()
    for r in per_rule:
        code = r["rule"][:4]
        fired.add(code)
        emit("5. Data quality", INFO,
             f"{RULE_CATEGORY.get(code, '?')} · {r['rule']}", f"{r['n']:,}")

    silent = sorted(set(RULE_CATEGORY) - fired)
    emit("5. Data quality", INFO, "Rules with zero failures", len(silent),
         ", ".join(silent) if silent else "all rules fired")

    # A rule that fired this month but not last is usually a source change.
    prev_ym = this_ym - 1
    prev_rules = q(f"""
        SELECT DISTINCT TRIM(reason) AS rule
        FROM {CATALOG}.silver.silver_flights_rejected
        LATERAL VIEW EXPLODE(SPLIT(rejection_reason, '\\\\|')) t AS reason
        WHERE CAST(ingest_year AS INT) * 12 + CAST(ingest_month AS INT) = {prev_ym}
    """)
    prev_codes = {r["rule"][:4] for r in prev_rules}
    new_codes = sorted(fired - prev_codes) if prev_codes else []
    if new_codes:
        emit("5. Data quality", WARN, "New failure modes", len(new_codes),
             "fired this month but not last: " + ", ".join(new_codes))

data_quality()

# COMMAND ----------

# MAGIC %md ## Section 6 — Pipeline execution

@section("6. Pipeline")
def pipeline_execution():
    if not LATEST_UPDATE:
        emit("6. Pipeline", WARN, "No pipeline update found", "—")
        return

    plans = q(f"""
        SELECT message FROM event_log(TABLE({EVENT_LOG_ANCHOR}))
        WHERE event_type = 'planning_information'
          AND origin.update_id = '{LATEST_UPDATE}'
        ORDER BY timestamp
    """)
    recomputes = []
    for r in plans:
        msg = r["message"]
        flow = msg.split("'")[1] if "'" in msg else msg[:60]
        technique = msg.split("executed as ")[-1].split(".")[0] if "executed as " in msg else "?"
        if technique == "COMPLETE_RECOMPUTE":
            recomputes.append(flow)
        emit("6. Pipeline", INFO, flow, technique)

    # Gold falling back to full recompute is the regression this pipeline was
    # rebuilt to eliminate — surface it rather than letting it be rediscovered
    # as "the pipeline feels slow again".
    gold_recompute = [f for f in recomputes if ".gold." in f]
    if gold_recompute:
        emit("6. Pipeline", WARN, "Gold full recomputes", len(gold_recompute),
             "incremental refresh regressed: " + ", ".join(gold_recompute[:6]))

    durations = q(f"""
        SELECT origin.flow_name AS flow,
               ROUND(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)), 1) AS secs,
               SUM(TRY_CAST(details:flow_progress:metrics:num_output_rows AS BIGINT)) AS rows_out
        FROM event_log(TABLE({EVENT_LOG_ANCHOR}))
        WHERE event_type = 'flow_progress' AND origin.update_id = '{LATEST_UPDATE}'
          AND origin.flow_name IS NOT NULL
        GROUP BY origin.flow_name ORDER BY secs DESC
    """)
    for r in durations:
        emit("6. Pipeline", INFO, f"{r['flow']} · duration",
             f"{r['secs']}s", f"{(r['rows_out'] or 0):,} rows written")

    failures = q(f"""
        SELECT message FROM event_log(TABLE({EVENT_LOG_ANCHOR}))
        WHERE origin.update_id = '{LATEST_UPDATE}' AND level = 'ERROR'
        ORDER BY timestamp DESC LIMIT 5
    """)
    emit("6. Pipeline", ERROR if failures else INFO, "Flow errors", len(failures),
         failures[0]["message"][:300] if failures else "none")

pipeline_execution()

# COMMAND ----------

# MAGIC %md ## Section 7 — Gold outputs

@section("7. Gold")
def gold_outputs():
    silver_max = one(f"""
        SELECT MAX(CAST(ingest_year AS INT) * 12 + CAST(ingest_month AS INT))
        FROM {CATALOG}.silver.silver_flights_clean
    """, 0)
    for t in GOLD_TABLES:
        try:
            n = one(f"SELECT COUNT(*) FROM {CATALOG}.gold.{t}", 0)
            mx = one(f"SELECT MAX(year * 12 + month) FROM {CATALOG}.gold.{t}", 0)
            lagging = silver_max and mx and mx < silver_max
            emit("7. Gold", WARN if (n == 0 or lagging) else INFO, t, f"{n:,} rows",
                 "EMPTY" if n == 0
                 else f"lags Silver by {silver_max - mx} month(s)" if lagging
                 else f"through {ym_label(mx // 12, mx % 12 or 12)}")
        except Exception as e:
            emit("7. Gold", WARN, t, "unreadable", str(e)[:200])

gold_outputs()

# COMMAND ----------

# MAGIC %md ## Section 8 — Volume anomaly
# MAGIC
# MAGIC A truncated or partially-downloaded file passes every row-level DQ rule,
# MAGIC because each individual row in it is perfectly valid. Only the aggregate
# MAGIC volume gives it away.

@section("8. Volume")
def volume_anomaly():
    vols = q(f"""
        SELECT year * 12 + month AS ym, SUM(total_flights) AS flights
        FROM {CATALOG}.gold.airline_monthly_kpi GROUP BY 1 ORDER BY 1
    """)
    if len(vols) < 2:
        emit("8. Volume", INFO, "Insufficient history", len(vols),
             "need 2+ months to compare")
        return

    latest, prior = vols[-1], vols[:-1][-6:]
    avg = sum(r["flights"] for r in prior) / len(prior)
    dev = (latest["flights"] - avg) / avg if avg else 0.0
    sev = (ERROR if abs(dev) >= VOLUME_DEVIATION_FAIL
           else WARN if abs(dev) >= VOLUME_DEVIATION_WARN else INFO)
    emit("8. Volume", sev, "Flights vs trailing average", f"{dev * 100:+.1f}%",
         f"{latest['flights']:,} this month · {avg:,.0f} avg of previous {len(prior)}"
         + (" — possible partial file" if sev != INFO else ""))

    # Carriers present last month and absent now.
    a, b = vols[-1]["ym"], vols[-2]["ym"]
    gone = q(f"""
        SELECT reporting_airline FROM {CATALOG}.gold.airline_monthly_kpi
        WHERE year * 12 + month = {b}
        EXCEPT
        SELECT reporting_airline FROM {CATALOG}.gold.airline_monthly_kpi
        WHERE year * 12 + month = {a}
    """)
    emit("8. Volume", WARN if gone else INFO, "Carriers dropped out", len(gone),
         ", ".join(r["reporting_airline"] for r in gone[:10]) if gone else "none")

volume_anomaly()

# COMMAND ----------

# MAGIC %md ## Section 1 — Verdict (computed last, rendered first)

errors = sum(1 for r in rows if r["severity"] == ERROR)
warns = sum(1 for r in rows if r["severity"] == WARN)
overall = ERROR if errors else WARN if warns else INFO
verdict = {ERROR: "FAIL", WARN: "WARN", INFO: "OK"}[overall]

header = [
    {"section": "1. Verdict", "severity": overall, "metric": "Overall status",
     "value": verdict, "detail": f"{errors} error(s), {warns} warning(s)"},
    {"section": "1. Verdict", "severity": INFO, "metric": "Pipeline update",
     "value": LATEST_UPDATE or "—", "detail": ""},
    {"section": "1. Verdict", "severity": INFO, "metric": "Acquisition run",
     "value": LATEST_ACQ_RUN or "—", "detail": ""},
    {"section": "1. Verdict", "severity": INFO, "metric": "Generated at",
     "value": GENERATED_AT.strftime("%Y-%m-%d %H:%M UTC"), "detail": ""},
]
rows = header + rows

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType,
)

# Declared explicitly rather than inferred. Inference from Row objects fails
# outright if any column comes back all-NULL, and — more quietly — saveAsTable
# in append mode matches columns by POSITION, so a Row field order that differs
# from the table's would write values into the wrong columns without erroring.
# The explicit schema plus the .select() below pins both.
SUMMARY_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("generated_at", TimestampType()),
    StructField("section", StringType()),
    StructField("seq", IntegerType()),
    StructField("severity", StringType()),
    StructField("severity_rank", IntegerType()),
    StructField("metric", StringType()),
    StructField("value", StringType()),
    StructField("detail", StringType()),
])
COLUMNS = [f.name for f in SUMMARY_SCHEMA.fields]

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
        run_id STRING, generated_at TIMESTAMP, section STRING, seq INT,
        severity STRING, severity_rank INT, metric STRING, value STRING,
        detail STRING
    ) USING DELTA
""")

if not rows:
    raise RuntimeError(
        "Run summary produced no rows — every section failed to emit. "
        "Check the cell output above for tracebacks."
    )

data = [
    (
        str(RUN_ID),
        GENERATED_AT,
        str(r["section"]),
        int(i),
        str(r["severity"]),
        int(RANK[r["severity"]]),
        str(r["metric"]),
        str(r["value"]),
        str(r["detail"]),
    )
    for i, r in enumerate(rows)
]

df = spark.createDataFrame(data, schema=SUMMARY_SCHEMA)
df.select(*COLUMNS).write.mode("append").saveAsTable(SUMMARY_TABLE)

print(f"{verdict}: {errors} error(s), {warns} warning(s), {len(rows)} rows → {SUMMARY_TABLE}")

# The task succeeds even on a FAIL verdict — the alert is the notification
# channel, and failing the task here would mask the report behind a job failure
# email that says nothing about what went wrong.
