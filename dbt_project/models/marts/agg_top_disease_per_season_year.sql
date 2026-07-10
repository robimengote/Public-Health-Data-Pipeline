{{ config(
    materialized='table',
    schema='cdc_marts',
    hours_to_expiration=8760
) }}

WITH base AS (
    SELECT
        dl.state_name,
        dl.state_abbreviation,
        dl.region_name,
        dl.census_division,
        dd.year,
        dd.week,        
        dd.season,
        dd.quarter,
        di.disease_name_short,
        di.disease_group,
        di.disease_category,
        di.severity_or_stage,
        COALESCE(fdc.current_week_cases, 0)     AS current_week_cases,
        COALESCE(fdc.previous_52_weeks_max, 0)  AS previous_52_weeks,
        fdc.current_week_cases_flag,
        CASE
            WHEN COALESCE(fdc.current_week_cases, 0) >= COALESCE(fdc.previous_52_weeks_max, 0)
            AND COALESCE(fdc.previous_52_weeks_max, 0) > 0
            THEN 1 ELSE 0
        END AS is_outbreak_week
    FROM {{ ref('fact_disease_cases') }} fdc
    LEFT JOIN {{ ref('dim_location') }} dl ON fdc.location_id = dl.location_id
    LEFT JOIN {{ ref('dim_date') }} dd ON fdc.date_id = dd.date_id
    LEFT JOIN {{ ref('dim_diseases') }} di ON fdc.disease_id = di.disease_id
    WHERE dd.year >= 2022 AND dd.year <= 2026
),

aggregated AS (
    SELECT
        year,
        season,
	week,
        state_name,
        state_abbreviation,
        region_name,
        census_division,
        disease_name_short,
        disease_group,
        disease_category,
        severity_or_stage,
        SUM(current_week_cases)                                         AS total_cases,
        SUM(previous_52_weeks)                                          AS previous_52_weeks_max,
        SUM(is_outbreak_week)                                           AS outbreak_week_count,
        COUNT(*)                                                        AS total_week_observations,
        COUNTIF(current_week_cases_flag IN ('U', 'N', 'NN', 'NP', 'NC')) AS flagged_week_count
    FROM base
    GROUP BY
        year,
        season,
	week,
        state_name,
        state_abbreviation,
        region_name,
        census_division,
        disease_name_short,
        disease_group,
        disease_category,
        severity_or_stage
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY state_name, year
            ORDER BY total_cases DESC
        ) AS disease_rank_in_state
    FROM aggregated
)

SELECT
    year,
    season,
    week,
    state_name,
    state_abbreviation,
    region_name,
    census_division,
    disease_name_short,
    disease_group,
    disease_category,
    severity_or_stage,
    CAST(ROUND(total_cases, 0) AS INT64)            AS total_cases,
    CAST(ROUND(previous_52_weeks_max, 0) AS INT64)  AS previous_52_weeks_max,
    outbreak_week_count,
    total_week_observations,
    flagged_week_count,
    disease_rank_in_state,
    CONCAT(state_name, '-', CAST(year AS STRING))   AS state_year_key
FROM ranked