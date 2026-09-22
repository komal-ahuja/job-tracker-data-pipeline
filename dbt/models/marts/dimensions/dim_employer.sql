WITH employers as(
    SELECT DISTINCT 
        TRIM(UPPER(COALESCE(employer_name, 'Unknown'))) as employer_name
    from {{ref('int_jobs')}}
)
Select 
{{dbt_utils.generate_surrogate_key(['employer_name'])}} as employer_sk,
employer_name,
from employers