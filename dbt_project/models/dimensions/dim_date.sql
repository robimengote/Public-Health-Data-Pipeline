{{ config(
    materialized='table',
    schema='cdc_dims'
) }}

WITH year_week_spine AS (
    SELECT DISTINCT
        CAST(year AS INT64) AS year,
        CAST(week AS INT64) AS week
    FROM {{ ref('stg_nndss_weekly') }}
),

enriched AS (
    SELECT
        -- Surrogate Key
        CAST(CONCAT(CAST(y.year AS STRING), LPAD(CAST(y.week AS STRING), 2, '0')) AS INT64)     AS date_id,

        y.year,
        y.week,

        -- Human readable period
        CONCAT(CAST(y.year AS STRING), '-W', LPAD(CAST(y.week AS STRING), 2, '0'))              AS mmwr_period,

        -- Human readable label
        CONCAT('Week ', CAST(y.week AS STRING), ', ', CAST(y.year AS STRING))                   AS week_label,

        -- Quarter
        CASE
            WHEN y.week BETWEEN 1  AND 13 THEN 'Q1'
            WHEN y.week BETWEEN 14 AND 26 THEN 'Q2'
            WHEN y.week BETWEEN 27 AND 39 THEN 'Q3'
            WHEN y.week BETWEEN 40 AND 52 THEN 'Q4'
            ELSE NULL
        END AS quarter,

        -- Half Year
        CASE
            WHEN y.week BETWEEN 1  AND 26 THEN 'H1'
            WHEN y.week BETWEEN 27 AND 52 THEN 'H2'
            ELSE NULL
        END AS half_year,

        -- Season
        CASE
            WHEN y.week BETWEEN 14 AND 26 THEN 'SPRING'
            WHEN y.week BETWEEN 27 AND 39 THEN 'SUMMER'
            WHEN y.week BETWEEN 40 AND 48 THEN 'FALL'
            ELSE 'WINTER'
        END AS season,

        -- Holiday columns
        CASE
            WHEN h.holiday_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_holiday_week,

        COALESCE(h.holiday_name, 'N/A')                                                         AS holiday_name,

    FROM year_week_spine y
    LEFT JOIN {{ ref('us_holidays') }} h
        ON y.year = h.year
        AND y.week = h.week
)

SELECT * FROM enriched
ORDER BY year, week