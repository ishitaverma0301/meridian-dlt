# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Fetch BTS Months
# MAGIC
# MAGIC Lands raw BTS On-Time Performance monthly ZIPs as CSV files in the
# MAGIC `raw_landing` Volume. This is the **only** job of this task — it does not
# MAGIC write to any Bronze/Silver table. The downstream Lakeflow Declarative
# MAGIC Pipeline (`src/pipeline/flights_pipeline.py`) owns everything from the
# MAGIC Volume onward.
# MAGIC
# MAGIC Design choice: there is **no separate watermark table**. "What's already
# MAGIC landed" is derived by listing the Volume itself on every run. That makes
# MAGIC this task idempotent and safe to run concurrently or re-trigger.

# COMMAND ----------

dbutils.widgets.dropdown("reprocess", "False", ["True", "False"], "Reprocess Mode")
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM, reprocess only)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM, reprocess only)")

REPROCESS = dbutils.widgets.get("reprocess").strip().lower() == "true"
START_DATE_PARAM = dbutils.widgets.get("start_date").strip() or None
END_DATE_PARAM = dbutils.widgets.get("end_date").strip() or None

CATALOG = "meridian"
BASE_URL = "https://transtats.bts.gov/PREZIP"
FILE_PREFIX = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
MIN_FILE_SIZE = 500_000
HTTP_TIMEOUT = 60
MAX_RETRIES = 3
BASELINE = "2025-01"

# BTS reliably publishes a month's data ~6-7 weeks after month end. Rather
# than hand-rolling attempt-count escalation to tell "not published yet"
# apart from "actually broken", the incremental window simply never reaches
# for months younger than this lag by default — it self-heals for free once
# BTS actually publishes, with zero persisted retry state.
PUBLISH_LAG_MONTHS = 2

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_landing"
EVENTS_TABLE = f"{CATALOG}.ops.ingestion_events"

import uuid
from datetime import datetime, timezone

RUN_ID = str(uuid.uuid4())
CURRENT_YM = datetime.now(timezone.utc).strftime("%Y-%m")

# COMMAND ----------

# MAGIC %md ### YYYY-MM helpers

# COMMAND ----------

def validate_ym(value, label="value"):
    try:
        datetime.strptime(value, "%Y-%m")
    except (ValueError, TypeError):
        raise ValueError(f"{label} must be YYYY-MM, got: {value!r}")

def next_ym(ym):
    y, m = map(int, ym.split("-"))
    m += 1
    if m > 12:
        m, y = 1, y + 1
    return f"{y:04d}-{m:02d}"

def shift_ym(ym, months):
    y, m = map(int, ym.split("-"))
    idx = (y * 12 + (m - 1)) - months
    return f"{idx // 12:04d}-{(idx % 12) + 1:02d}"

def ym_range(start_ym, end_ym):
    validate_ym(start_ym, "range start")
    validate_ym(end_ym, "range end")
    cur = start_ym
    while cur <= end_ym:
        yield cur
        cur = next_ym(cur)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover already-landed months
# MAGIC
# MAGIC  The Volume itself is the source
# MAGIC of truth for "what have we already fetched"

# COMMAND ----------

def discover_landed_months():
    landed = set()
    try:
        year_dirs = dbutils.fs.ls(RAW_VOLUME_PATH)
    except Exception:
        return landed
    for yd in year_dirs:
        if not yd.name.startswith("year="):
            continue
        year = yd.name.rstrip("/").split("=", 1)[1]
        try:
            month_dirs = dbutils.fs.ls(yd.path)
        except Exception:
            continue
        for md in month_dirs:
            if not md.name.startswith("month="):
                continue
            month = md.name.rstrip("/").split("=", 1)[1]
            try:
                exists = any(
                    f.path.endswith(f"flights_{year}_{month}.csv")
                    for f in dbutils.fs.ls(md.path)
                )
            except Exception:
                exists = False
            if exists:
                landed.add(f"{year}-{month}")
    return landed

def resolve_window():
    landed = discover_landed_months()
    if REPROCESS:
        if not START_DATE_PARAM:
            raise ValueError("reprocess=True requires start_date.")
        start_ym = START_DATE_PARAM
        end_ym = END_DATE_PARAM or CURRENT_YM
        validate_ym(start_ym, "start_date")
        validate_ym(end_ym, "end_date")
        if start_ym > end_ym:
            raise ValueError(f"start_date ({start_ym}) after end_date ({end_ym}).")
        return start_ym, end_ym, landed

    start_ym = next_ym(max(landed)) if landed else BASELINE
    end_ym = shift_ym(CURRENT_YM, PUBLISH_LAG_MONTHS)
    if end_ym < start_ym:
        end_ym = start_ym  # nothing new is expected to be publishable yet
    return start_ym, end_ym, landed

# COMMAND ----------

# MAGIC %md ### Download & Land One Month

# COMMAND ----------

import urllib.request, urllib.error, zipfile, os, time

def download_with_retry(url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp, open(dest_path, "wb") as out:
                n = 0
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    out.write(chunk)
                    n += len(chunk)
            return n
        except urllib.error.HTTPError as e:
            if e.code in (403, 404) or attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)
        except (urllib.error.URLError, OSError):
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)

def fetch_month(year, month):
    month_key = f"{year}-{month}"
    zip_filename = f"{FILE_PREFIX}_{year}_{int(month)}.zip"
    url = f"{BASE_URL}/{zip_filename}"
    tmp_zip = f"/tmp/flights_{year}_{month}.zip"
    dest_dir = f"{RAW_VOLUME_PATH}/year={year}/month={month}"
    dest_csv = f"{dest_dir}/flights_{year}_{month}.csv"

    print(f"  FETCHING {month_key} <- {url}")
    try:
        n = download_with_retry(url, tmp_zip)
        if n < MIN_FILE_SIZE:
            print(f"  [NOT_PUBLISHED] {month_key} — file too small, BTS hasn't published this month yet.")
            return "not_published", "File too small (not published yet)"

        os.makedirs(dest_dir, exist_ok=True)
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            csv_names = [x for x in zf.namelist() if x.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError("No CSV in ZIP")
            with zf.open(csv_names[0]) as src, open(dest_csv, "wb") as dst:
                dst.write(src.read())

        csv_mb = os.path.getsize(dest_csv) / 1024 / 1024
        if csv_mb < (MIN_FILE_SIZE / 1024 / 1024):
            os.remove(dest_csv)
            print(f"  [NOT_PUBLISHED] {month_key} — extracted CSV too small ({csv_mb:.2f} MB).")
            return "not_published", f"CSV too small ({csv_mb:.2f} MB)"

        print(f"  [LANDED] {month_key} — {csv_mb:.1f} MB -> {dest_csv}")
        return "landed", f"{csv_mb:.1f} MB"

    except urllib.error.HTTPError as e:
        if e.code == 404 or 500 <= e.code < 600:
            print(f"  [NOT_PUBLISHED] {month_key} — HTTP {e.code}.")
            return "not_published", f"HTTP {e.code}"
        print(f"  [FAILED] {month_key} — HTTP {e.code}.")
        return "failed", f"HTTP {e.code}"
    except urllib.error.URLError as e:
        print(f"  [NOT_PUBLISHED] {month_key} — URLError: {e.reason}.")
        return "not_published", f"URLError: {e.reason}"
    except zipfile.BadZipFile:
        print(f"  [NOT_PUBLISHED] {month_key} — corrupt ZIP (BTS often republishes within a day or two).")
        return "not_published", "Corrupt ZIP"
    except Exception as e:
        print(f"  [FAILED] {month_key} — {type(e).__name__}: {e}")
        return "failed", f"{type(e).__name__}: {e}"
    finally:
        if os.path.exists(tmp_zip):
            os.remove(tmp_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event log (observability only — never read back to drive control flow)
# MAGIC
# MAGIC This is append-only and exists purely so a Lakeview dashboard / SQL alert
# MAGIC can show ingestion health. Nothing in this notebook re-reads it to decide
# MAGIC what to do next.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
        run_id STRING, year STRING, month STRING,
        status STRING, detail STRING, event_time TIMESTAMP
    ) USING DELTA
""")

def log_events(rows):
    from pyspark.sql import Row
    if not rows:
        return
    df = spark.createDataFrame([
        Row(run_id=RUN_ID, year=y, month=m, status=s, detail=d, event_time=datetime.now(timezone.utc))
        for (y, m, s, d) in rows
    ])
    df.write.mode("append").saveAsTable(EVENTS_TABLE)

# COMMAND ----------

# MAGIC %md ### Main

# COMMAND ----------

def main():
    start_ym, end_ym, landed = resolve_window()
    mode = "reprocess" if REPROCESS else "incremental"
    print(f"Mode: {mode.upper()} | window: {start_ym} -> {end_ym} | already landed: {len(landed)} month(s)")

    counts = {"landed": 0, "skipped": 0, "not_published": 0, "failed": 0}
    events = []

    for month_key in ym_range(start_ym, end_ym):
        year, month = month_key.split("-")
        if not REPROCESS and month_key in landed:
            counts["skipped"] += 1
            continue
        status, detail = fetch_month(year, month)
        counts[status] += 1
        events.append((year, month, status, detail))

    log_events(events)
    print(f"\nCounts: {counts}")

    if counts["failed"] > 0:
        raise RuntimeError(
            f"{counts['failed']} month(s) failed with a non-transient error — see {EVENTS_TABLE}. "
            f"Lakeflow Job retry/notification policy should handle alerting from here."
        )

main()