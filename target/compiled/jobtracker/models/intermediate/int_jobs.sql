

With int_jobs as (
    Select * from JOB_DB.STAGING.stg_jobs

where source_ingest_timestamp > (Select max(scraped_at) from JOB_DB.INTERMEDIATE.int_jobs)

)
Select 
-- job_uid is not globally unique across all jobs.
-- Validation showed collisions across different employers.
-- However, (job_uid, employer_name) uniquely identifies a logical job:
--   • one job_title per combination
--   • one location per combination
--   • multiple publishers correctly map to the same logical job
--
-- Therefore this combination is used to generate job_sk.

md5(cast(coalesce(cast(job_uid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(employer_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as job_posting_sk,
job_uid,
job_id,                 
employer_name,
job_title,
job_publisher,          
job_city,
job_state,
job_country,
job_description,

min_salary,
max_salary,
salary_range,
salary_period,

job_posted_at,
employment_type,
is_remote,

job_apply_link,
source_ingest_timestamp as scraped_at,
search_query as search_term
from int_jobs
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
        job_uid, 
        employer_name,
        source_ingest_timestamp
    order by job_publisher desc) = 1