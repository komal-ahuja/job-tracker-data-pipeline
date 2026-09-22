WITH exclusions AS (
    SELECT
        skill_name,
        LISTAGG(
            '(' || exclusion_regex || ')',
            '|'
        ) WITHIN GROUP (ORDER BY exclusion_regex) AS combined_exclusion_regex
    FROM {{ ref('skill_exclusions') }}
    WHERE exclusion_regex IS NOT NULL
      AND TRIM(exclusion_regex) <> ''
    GROUP BY skill_name
),
skill_matches AS (
    SELECT
        j.job_posting_sk,
        s.skill_name
    FROM {{ ref('int_jobs') }} j
    JOIN {{ ref('skill_lookup') }} s
        ON (
            (
                s.match_type = 'standard'
                AND REGEXP_LIKE(
                    LOWER(j.job_description),
                    '.*\\b' || LOWER(s.match_text) || '\\b.*',
                    'is'
                )
            )
            OR
            (
                s.match_type = 'contextual'
                AND REGEXP_LIKE(
                    j.job_description,
                    '.*\\b' || s.match_text || '\\b.*',
                    's'
                )
            )
        )
    LEFT JOIN exclusions e
        ON e.skill_name = s.skill_name
    WHERE NOT COALESCE(
        REGEXP_LIKE(
            j.job_description,
            e.combined_exclusion_regex,
            's'
        ),
        FALSE
    )
)
SELECT DISTINCT
    job_posting_sk,
   {{dbt_utils.generate_surrogate_key(['skill_name'])}}as skill_sk
FROM skill_matches