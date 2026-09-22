WITH source_combo AS (
    SELECT DISTINCT
        COALESCE(job_city, 'UNKNOWN') AS job_city,
        COALESCE(job_state, 'UNKNOWN') AS job_state,
        COALESCE(job_country, 'UNKNOWN') AS job_country
    FROM {{ ref('int_jobs') }}
),

dim_combo AS (
    SELECT DISTINCT
        UPPER(job_city) AS job_city,
        UPPER(job_state) AS job_state,
        UPPER(job_country) AS job_country
    FROM {{ ref('dim_location') }}
)

SELECT
    s.*
FROM source_combo s
LEFT JOIN dim_combo l
    ON UPPER(s.job_city) = l.job_city
    AND UPPER(s.job_state) = l.job_state
    AND UPPER(s.job_country) = l.job_country
WHERE l.job_city IS NULL