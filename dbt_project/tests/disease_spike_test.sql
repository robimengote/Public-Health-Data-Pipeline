{{ config(severity='warn') }}


SELECT
    s.*
FROM {{ ref('stg_nndss_weekly') }} s
WHERE current_week_cases IS NOT NULL
  AND previous_52_weeks_max IS NOT NULL
  AND previous_52_weeks_max > 0
  AND current_week_cases >= previous_52_weeks_max