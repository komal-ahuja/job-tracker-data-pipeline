WITH classified_jobs AS (
    SELECT
        f.job_posting_sk,
        f.remote_flag,
        f.job_title,
        CASE 
            WHEN LOWER(f.job_title) LIKE '%analytics engineer%'
                THEN 'Analytics Engineer'

            WHEN LOWER(f.job_title) LIKE '%data engineer%'
                OR LOWER(f.job_title) LIKE '%data & ai engineer%'
                THEN 'Data Engineer'

            WHEN LOWER(f.job_title) LIKE '%business intelligence engineer%'
                OR LOWER(f.job_title) LIKE '%bi engineer%'
                THEN 'BI Engineer'

            ELSE 'Other'
        END AS job_role
    FROM {{ ref('fct_job_postings_snapshot') }} f
),

role_totals AS (
    SELECT
        job_role,
        COUNT(DISTINCT job_posting_sk) AS total_postings
    FROM classified_jobs
    WHERE job_role IN (
        'Data Engineer',
        'Analytics Engineer',
        'BI Engineer'
    )
    GROUP BY job_role
),

remote_totals AS (
    SELECT
        job_role,
        COUNT(DISTINCT job_posting_sk) AS remote_postings
    FROM classified_jobs
    WHERE job_role IN (
        'Data Engineer',
        'Analytics Engineer',
        'BI Engineer'
    )
    AND remote_flag = TRUE
    GROUP BY job_role
)

SELECT
    r.job_role,
    r.total_postings,
    COALESCE(m.remote_postings, 0) AS remote_postings,
    ROUND(
        100.0 * COALESCE(m.remote_postings, 0)
        / NULLIF(r.total_postings, 0),
        2
    ) AS remote_percentage
FROM role_totals r
LEFT JOIN remote_totals m
    ON r.job_role = m.job_role
ORDER BY remote_percentage DESC