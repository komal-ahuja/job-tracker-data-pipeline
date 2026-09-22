WITH locations as(
    Select distinct
    TRIM(UPPER(COALESCE(job_city, 'Unknown'))) AS job_city,
    TRIM(UPPER(COALESCE(job_state, 'Unknown'))) As job_state,
    TRIM(UPPER(COALESCE(job_country, 'Unknown'))) As job_country
     FROM {{ ref('int_jobs') }}
)
Select 
{{dbt_utils.generate_surrogate_key(['job_city', 'job_state', 'job_country'])}} as location_sk,
    job_city,
    job_state,
    job_country
from locations



