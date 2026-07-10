SELECT
    s.*
FROM {{ ref('stg_nndss_weekly') }} s
WHERE (CAST(s.year AS INT64) * 100 + CAST(s.week AS INT64)) 
    > (EXTRACT(YEAR FROM CURRENT_DATE()) * 100 + EXTRACT(WEEK FROM CURRENT_DATE()))