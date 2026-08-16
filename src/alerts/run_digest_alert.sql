-- Query behind the `meridian_run_digest` SQL Alert.
--
-- WHY THIS EXISTS. The v2 alerts engine does not interpolate query results into
-- the notification body — {{QUERY_RESULT_TABLE}} and {{#QUERY_RESULT_ROWS}} come
-- through the email literally, Markdown-escaped. The only part of the query that
-- reaches the inbox is the Evaluation line, which prints the condition's COLUMN
-- NAME and its EVALUATED VALUE:
--
--     MAX(severity_rank)   2.0   >=   static value   2.0
--
-- Both halves are ours to choose. So instead of putting a bare integer there,
-- this collapses the entire run into one string column. Threshold on it and the
-- email carries the report itself.
--
-- SETUP
--   Condition:  Max  ·  run_digest  ·  !=  ·  static value  ·  OK
--   (a digest never literally equals "OK", so this always evaluates true — set
--   "When alerting, notify" to Each time and it sends every run)
--
-- IF THE CONDITION PANEL WON'T ACCEPT A STRING COLUMN, fall back to the numeric
-- `errors` / `warnings` columns also returned below. The email then shows
-- "MAX(warnings) 2.0", which at least carries a real number — and pair it with a
-- descriptive alert NAME, since the name is the other thing that reaches the
-- inbox unmangled.
--
-- Digest length is the risk: the Evaluation box may truncate. Issues are ordered
-- by `seq`, so the verdict and the most significant sections come first and any
-- truncation loses the tail rather than the headline.

WITH latest AS (
  SELECT MAX(generated_at) AS g FROM meridian.ops.run_summary
),
r AS (
  SELECT s.*
  FROM meridian.ops.run_summary s
  JOIN latest l ON s.generated_at = l.g
),
agg AS (
  SELECT
    MAX(CASE WHEN metric = 'Overall status'  THEN value END) AS verdict,
    SUM(CASE WHEN severity = 'ERROR' THEN 1 ELSE 0 END)      AS errors,
    SUM(CASE WHEN severity = 'WARN'  THEN 1 ELSE 0 END)      AS warnings,
    MAX(CASE WHEN metric = 'Month evaluated' THEN value END) AS month_evaluated,
    MAX(CASE WHEN metric = 'Rows evaluated'  THEN value END) AS rows_evaluated,
    MAX(CASE WHEN metric = 'Rejection rate'  THEN value END) AS rejection_rate
  FROM r
),
-- Every non-green row, in report order. ARRAY_SORT on a struct orders by its
-- first field, so packing `seq` in front preserves the report's own sequence —
-- COLLECT_LIST alone gives no ordering guarantee.
issues AS (
  SELECT ARRAY_JOIN(
           TRANSFORM(
             ARRAY_SORT(
               COLLECT_LIST(
                 STRUCT(seq AS s, CONCAT(section, ' / ', metric, ' = ', value) AS t)
               )
             ),
             x -> x.t
           ),
           ' • '
         ) AS issue_text
  FROM r
  WHERE severity <> 'INFO'
)
-- Every CONCAT argument is cast to STRING explicitly. `errors` and `warnings`
-- are BIGINT, and under ANSI mode — the default on current Databricks — CONCAT
-- does not implicitly cast them, so the query fails outright and the alert's
-- Condition dropdown is left with no columns to offer.
SELECT
  CONCAT_WS(' · ',
    CONCAT('VERDICT ', COALESCE(a.verdict, '?')),
    CONCAT(CAST(a.errors AS STRING), ' err / ',
           CAST(a.warnings AS STRING), ' warn'),
    CONCAT('month ',    COALESCE(a.month_evaluated, '?')),
    CONCAT('rows ',     COALESCE(a.rows_evaluated, '?')),
    CONCAT('rejected ', COALESCE(a.rejection_rate, '?')),
    COALESCE(i.issue_text, 'no issues')
  )            AS run_digest,
  a.errors,
  a.warnings
FROM agg a
CROSS JOIN issues i
