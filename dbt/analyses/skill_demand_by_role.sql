-- analyses/skill_demand_by_role.sql
--
-- Measures the percentage of job postings within each role
-- that mention a given skill.
--
-- Target roles:
--   - Data Engineer
--   - Analytics Engineer
--   - BI Engineer

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
),

skills AS (
    SELECT 
        d.skill_name,
        j.job_role,
        COUNT(DISTINCT j.job_posting_sk) AS jobs_pr_skill
    FROM classified_jobs j
    JOIN {{ ref('bridge_job_skills') }} b
        ON j.job_posting_sk = b.job_sk
    JOIN {{ ref('dim_skills') }} d
        ON b.skill_sk = d.skill_sk
    WHERE j.job_role IN (
        'Data Engineer',
        'Analytics Engineer',
        'BI Engineer'
    )
    GROUP BY
        d.skill_name,
        j.job_role
),

job_totals AS (
    SELECT
        job_role,
        COUNT(DISTINCT job_posting_sk) AS jobs_pr_title
    FROM classified_jobs
    WHERE job_role IN (
        'Data Engineer',
        'Analytics Engineer',
        'BI Engineer'
    )
    GROUP BY job_role
),

skill_percentages AS (
    SELECT 
        s.skill_name,
        s.job_role,
        s.jobs_pr_skill,
        t.jobs_pr_title,
        COALESCE(ROUND(
            100.0 * s.jobs_pr_skill / NULLIF(t.jobs_pr_title, 0),
            2
        ), 0) AS skill_percentage
    FROM skills s
    JOIN job_totals t
        ON s.job_role = t.job_role
)

SELECT 
    skill_name AS skill,

    COALESCE(
        MAX(
            CASE 
                WHEN job_role = 'Data Engineer'
                    THEN skill_percentage
            END
        ), 0
    ) AS data_engineer,

    COALESCE(
        MAX(
            CASE 
                WHEN job_role = 'Analytics Engineer'
                    THEN skill_percentage
            END
        ), 0
    ) AS analytics_engineer,

    COALESCE(
        MAX(
            CASE 
                WHEN job_role = 'BI Engineer'
                    THEN skill_percentage
            END
        ), 0
    ) AS bi_engineer

FROM skill_percentages
GROUP BY skill_name
ORDER BY
    data_engineer DESC,
    analytics_engineer DESC,
    bi_engineer DESC
    