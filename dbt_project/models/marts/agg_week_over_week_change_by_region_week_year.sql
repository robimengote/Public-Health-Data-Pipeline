{{ config(
    materialized='table',
    schema='cdc_marts',
    hours_to_expiration=8760
) }}

WITH base AS (
    SELECT
        dl.region_name,
        dl.census_division,
        dd.year,
        dd.week,
        dd.mmwr_period,
        dd.week_label,
        dd.season,
        dd.quarter,
        dd.is_holiday_week,
        dd.holiday_name,
        di.disease_group,
        di.disease_name_short,
        di.disease_status,
        di.disease_category,
        di.severity_or_stage,
        COALESCE(fdc.current_week_cases, 0)                     AS current_week_cases,
        fdc.current_week_cases_flag
    FROM {{ ref('fact_disease_cases') }} fdc
    LEFT JOIN {{ ref('dim_location') }} dl
        ON fdc.location_id = dl.location_id
    LEFT JOIN {{ ref('dim_date') }} dd
        ON fdc.date_id = dd.date_id
    LEFT JOIN {{ ref('dim_diseases') }} di
        ON fdc.disease_id = di.disease_id
),

pre_aggregated AS (
    SELECT
        region_name,
        census_division,
        year,
        week,
        mmwr_period,
        week_label,
        season,
        quarter,
        is_holiday_week,
        holiday_name,
        disease_group,
        disease_name_short,
        disease_status,
        disease_category,
        severity_or_stage,
        COUNTIF(current_week_cases_flag != '-')                 AS flagged_current_week_count,
        SUM(current_week_cases)                                 AS current_week_cases
    FROM base
    GROUP BY
        region_name,
        census_division,
        year,
        week,
        mmwr_period,
        week_label,
        season,
        quarter,
        is_holiday_week,
        holiday_name,
        disease_group,
        disease_name_short,
        disease_status,
        disease_category,
        severity_or_stage
),

final_agg AS (
    SELECT
        *,
        COALESCE(
            LAG(current_week_cases) OVER (
                PARTITION BY region_name, disease_name_short
                ORDER BY year, week
            ), 0
        )                                                       AS actual_previous_week_cases,

        current_week_cases - COALESCE(
            LAG(current_week_cases) OVER (
                PARTITION BY region_name, disease_name_short
                ORDER BY year, week
            ), 0
        )                                                       AS wow_change,

        ROUND(
            SAFE_DIVIDE(
                current_week_cases - COALESCE(
                    LAG(current_week_cases) OVER (
                        PARTITION BY region_name, disease_name_short
                        ORDER BY year, week
                    ), 0
                ),
                NULLIF(
                    COALESCE(
                        LAG(current_week_cases) OVER (
                            PARTITION BY region_name, disease_name_short
                            ORDER BY year, week
                        ), 0
                    ), 0
                )
            ) * 100, 2
        )                                                       AS wow_pct_change

    FROM pre_aggregated
),

final AS (
    SELECT
        *,
        SUM(current_week_cases) OVER (
            PARTITION BY region_name, disease_name_short, year
            ORDER BY week
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                       AS running_cumulative_cases
    FROM final_agg
)

SELECT
census_division, 
region_name,
year,
quarter,
season,
week,
week_label,
mmwr_period,
is_holiday_week,
holiday_name,
disease_name_short,
disease_status,
disease_group,
disease_category,
severity_or_stage,
CAST(ROUND(current_week_cases, 0) AS INT64) 			AS current_week_cases,
CAST(ROUND(actual_previous_week_cases, 0) AS INT64)		AS actual_previous_week_cases,
COALESCE(wow_change, 0)						AS wow_change,
COALESCE(wow_pct_change, 0)					AS wow_pct_change,
flagged_current_week_count,
CAST(ROUND(running_cumulative_cases, 0) AS INT64)		AS running_cumulative_cases_year

FROM final
ORDER BY region_name ASC, disease_name_short ASC, year ASC, week ASC