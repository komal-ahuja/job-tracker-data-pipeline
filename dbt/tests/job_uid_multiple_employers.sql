WITH job_counts AS (

    SELECT
        job_uid,
        COUNT(DISTINCT employer_name) AS employer_count
    FROM {{ ref('int_jobs') }}
    WHERE job_uid IS NOT NULL
      AND employer_name IS NOT NULL
    GROUP BY job_uid
)
SELECT
    COUNT_IF(employer_count > 1)
        / NULLIF(COUNT(*), 0)::FLOAT AS multiple_employer_rate
FROM job_counts
HAVING multiple_employer_rate > 0.03