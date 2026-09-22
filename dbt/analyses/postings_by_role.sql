--Number of distinct job_posting captured by pipeline
WITH classified_jobs AS (
    SELECT
        f.job_posting_sk,
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
)

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
ORDER BY total_postings DESC