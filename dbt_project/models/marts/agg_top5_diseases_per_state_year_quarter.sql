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
        dd.season,
        dd.quarter,
        di.disease_name_short,
        di.disease_group,
        di.disease_category,
        di.severity_or_stage,
        COALESCE(fdc.current_week_cases, 0)     AS current_week_cases,
        fdc.current_week_cases_flag
    FROM {{ ref('fact_disease_cases') }} fdc
    LEFT JOIN {{ ref('dim_location') }} dl
        ON fdc.location_id = dl.location_id
    LEFT JOIN {{ ref('dim_date') }} dd
        ON fdc.date_id = dd.date_id
    LEFT JOIN {{ ref('dim_diseases') }} di
        ON fdc.disease_id = di.disease_id
),

aggregated AS (
    SELECT
        state_name,
        state_abbreviation,
        region_name,
        census_division,
        year,
        season,
        quarter,
        disease_name_short,
        disease_group,
        disease_category,
        severity_or_stage,
        SUM(current_week_cases)                 AS total_cases,
        COUNTIF(current_week_cases_flag != '-') AS flagged_week_count,
        COUNT(*)                                AS weeks_reported
    FROM base
    GROUP BY
        state_name,
        state_abbreviation,
        region_name,
        census_division,
        year,
        season,
        quarter,
        disease_name_short,
        disease_group,
        disease_category,
        severity_or_stage
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY state_name, year, quarter
            ORDER BY total_cases DESC
        ) AS disease_rank_in_state
    FROM aggregated
)

SELECT
state_name,
state_abbreviation,
region_name,
census_division,
year,
season,
quarter,
disease_name_short,
disease_group,
disease_category,
severity_or_stage,
CAST(ROUND(total_cases, 0) AS INT64) 	AS total_cases,
flagged_week_count,
weeks_reported,
disease_rank_in_state


FROM ranked
WHERE disease_rank_in_state <= 5
ORDER BY state_name ASC, year ASC, quarter ASC, disease_rank_in_state ASC