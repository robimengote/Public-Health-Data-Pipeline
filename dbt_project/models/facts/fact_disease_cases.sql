{{ config(
    materialized='table',
    schema='cdc_facts'
) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_nndss_weekly') }}
),

dim_location AS (
    SELECT location_id, state_name
    FROM {{ ref('dim_location') }}
),

dim_diseases AS (
    SELECT disease_id, disease_name_long
    FROM {{ ref('dim_diseases') }}
),

dim_date AS (
    SELECT date_id, year, week
    FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'stg.state_name',
            'stg.disease_name',
            'stg.year',
            'stg.week'
        ]) }}                                   AS fact_id,

        -- Foreign Keys
        l.location_id,
        d.disease_id,
        dt.date_id,

        -- Measures
        stg.current_week_cases,
        stg.previous_52_weeks_max,
        stg.cumulative_ytd_cases,
        stg.previous_year_cumulative,

        -- Flags
        stg.current_week_cases_flag,
        stg.previous_52_weeks_max_flag,
        stg.cumulative_ytd_cases_flag,
        stg.previous_year_cumulative_flag

    FROM stg
    LEFT JOIN dim_location l
        ON stg.state_name = l.state_name
    LEFT JOIN dim_diseases d
        ON TRIM(stg.disease_name) = TRIM(d.disease_name_long)
    LEFT JOIN dim_date dt
        ON stg.year = dt.year
        AND stg.week = dt.week
)

SELECT * FROM joined