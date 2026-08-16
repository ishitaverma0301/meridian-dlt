-- Query behind the `meridian_run_summary` SQL Alert.
--
-- Deliberately dumb: run_summary.py has already done all the work and
-- persisted it. This just selects the newest run and shapes it for the email.
--
-- NO CUSTOM EMAIL TEMPLATE. The alert runs on the v2 alerts engine, which does
-- not interpolate query results into the custom-message field — {{ALERT_NAME}},
-- {{QUERY_RESULT_TABLE}} and {{#QUERY_RESULT_ROWS}} all arrive in the email
-- literally, Markdown-escaped as {{ALERT\_NAME}}. Those variables belong to the
-- legacy alert system. The custom message is left empty and the default
-- notification carries the result table.
--
-- That is why this query does the presentation work: the 🔴/🟡/🟢 badge is a real
-- column produced below rather than template logic, and rows are pre-ordered by
-- `seq` so the default renderer shows the report in the right order. Formatting
-- that lives in SQL survives any renderer.
--
-- Alert condition: aggregate MAX on `severity_rank`.
--   >= 1  email every run (INFO and above)
--   >= 2  email only when something is amber or worse   <-- recommended
--   >= 3  email only on errors
--
-- The template renders at most 100 rows, hence the LIMIT — the report is
-- ordered worst-first inside each section by `seq`, so a truncated email still
-- leads with what matters.

-- Scoped on generated_at, NOT run_id. `run_id` is the pipeline update id, which
-- does not change when run_summary is re-run against the same update — so
-- filtering by it returns every report built for that update and the email shows
-- each row two or three times. `generated_at` is stamped once per notebook
-- execution and is identical across all rows of one report, so equality against
-- MAX() selects exactly one report.

WITH latest AS (
  SELECT MAX(generated_at) AS generated_at
  FROM meridian.ops.run_summary
)
SELECT
  CASE s.severity
    WHEN 'ERROR' THEN '🔴'
    WHEN 'WARN'  THEN '🟡'
    ELSE '🟢'
  END                AS status,
  s.section,
  s.metric,
  s.value,
  s.detail,
  s.severity_rank
FROM meridian.ops.run_summary s
JOIN latest l ON s.generated_at = l.generated_at
ORDER BY s.seq
LIMIT 100
