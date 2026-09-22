WITH source AS (

    SELECT *
    FROM {{ source('raw', 'RAW_JOBS_API') }}

),

flattened AS (

    SELECT
        filename AS file_name,
        source_ingest_timestamp,
        snowflake_load_timestamp,
        f.value AS job,
        raw_data:parameters.query::STRING AS search_query

    FROM source,
    LATERAL FLATTEN(input => raw_data:data) f

)

SELECT
    -- Metadata
    file_name,
    source_ingest_timestamp,
    snowflake_load_timestamp,

    -- Search context
    search_query,

    -- Job identifiers
    job:job_id::STRING AS job_id,
    job:job_uid::STRING AS job_uid,

    -- Job details
    job:job_title::STRING AS job_title,
    job:job_publisher::STRING AS job_publisher,
    job:employer_name::STRING AS employer_name,
   
    job:job_description::STRING AS job_description,

    -- Location
    job:job_city::STRING AS job_city,
    job:job_state::STRING AS job_state,
    job:job_country::STRING AS job_country,

    -- Compensation
    job:job_min_salary::NUMBER AS min_salary,
    job:job_max_salary::NUMBER AS max_salary,
    job:job_salary_string::STRING AS salary_range,
    job:job_salary_period::STRING AS salary_period,

    -- Job attributes
    job:job_posted_at_datetime_utc::TIMESTAMP_NTZ AS job_posted_at,
    job:job_employment_type::STRING AS employment_type,
    job:job_is_remote::BOOLEAN AS is_remote,

    -- Links
    job:job_apply_link::STRING AS job_apply_link

FROM flattened