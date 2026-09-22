--This test checks that every distinct (job_posting_sk, scraped_at) row in int_jobs is represented exactly once in the fact table.
-- Someone changes a LEFT JOIN back to INNER JOIN.
-- A dimension surrogate key stops being unique e.g., 
--a bug in dim_employer/dim_location produces two rows with the same employer_sk/location_sk 
--(or same join keys) causing one int_jobs row to match twice (inflates count too high).
WITH source_count AS (
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT DISTINCT
            job_posting_sk,
            scraped_at
        FROM {{ ref('int_jobs') }}
    ) x
),

fct_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('fct_job_postings_snapshot') }}
)

SELECT
    s.cnt AS expected_count,
    f.cnt AS actual_count
FROM source_count s
CROSS JOIN fct_count f
WHERE s.cnt != f.cnt