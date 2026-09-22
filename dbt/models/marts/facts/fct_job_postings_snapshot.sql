SELECT
    {{ dbt_utils.generate_surrogate_key([
        'i.job_posting_sk',
        'i.scraped_at'
    ]) }} AS fct_job_postings_sk,

    i.job_posting_sk,

    i.scraped_at,

    COALESCE(
    e.employer_sk,
    {{ dbt_utils.generate_surrogate_key(["TRIM(UPPER('Unknown'))"]) }}
    ) AS employer_sk,

   COALESCE(
    l.location_sk,
    {{ dbt_utils.generate_surrogate_key([
        "'UNKNOWN'",
        "'UNKNOWN'",
        "'UNKNOWN'"
    ]) }}
) AS location_sk,

    -- Job attributes
    i.job_title,
    i.employment_type,
    i.is_remote AS remote_flag,

    -- Compensation
    i.min_salary,
    i.max_salary,
    i.salary_range,
    i.salary_period,

    -- Posting metadata
    i.job_posted_at,
    i.job_apply_link,
    i.job_publisher,

    -- Snapshot tracking
    MIN(i.scraped_at::DATE)
        OVER (
            PARTITION BY i.job_posting_sk
        ) AS first_seen_date,

    MAX(i.scraped_at::DATE)
        OVER (
            PARTITION BY i.job_posting_sk
        ) AS last_seen_date,

    CASE
        WHEN i.scraped_at::TIMESTAMP =
             MAX(i.scraped_at::TIMESTAMP)
                 OVER (
                     PARTITION BY i.job_posting_sk
                 )
        THEN TRUE
        ELSE FALSE
    END AS is_active,


FROM {{ ref('int_jobs') }} i

LEFT JOIN {{ ref('dim_employer') }} e
    ON TRIM(UPPER(COALESCE(i.employer_name, 'Unknown'))) = e.employer_name

LEFT JOIN {{ ref('dim_location') }} l
    ON TRIM(UPPER(COALESCE(i.job_city, 'Unknown'))) = l.job_city
   AND TRIM(UPPER(COALESCE(i.job_state, 'Unknown'))) = l.job_state
   AND TRIM(UPPER(COALESCE(i.job_country, 'Unknown'))) = l.job_country