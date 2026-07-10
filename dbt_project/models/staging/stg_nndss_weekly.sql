WITH source AS (
    SELECT * FROM {{ source('raw', 'nndss_weekly') }}
),

cleaned AS (
    SELECT
        -- Identifiers (masked)
        MD5(sort_order)                                                             AS sort_order_masked,
        MD5(COALESCE(CAST(geocode_coordinates[SAFE_OFFSET(0)] AS STRING), ''))      AS longitude_masked,
        MD5(COALESCE(CAST(geocode_coordinates[SAFE_OFFSET(1)] AS STRING), ''))      AS latitude_masked,

        -- Location fields
        UPPER(TRIM(states))                                                         AS state_name,

        CASE UPPER(TRIM(states))
            WHEN 'CONNECTICUT'          THEN 'NEW ENGLAND'
            WHEN 'MAINE'                THEN 'NEW ENGLAND'
            WHEN 'MASSACHUSETTS'        THEN 'NEW ENGLAND'
            WHEN 'NEW HAMPSHIRE'        THEN 'NEW ENGLAND'
            WHEN 'RHODE ISLAND'         THEN 'NEW ENGLAND'
            WHEN 'VERMONT'              THEN 'NEW ENGLAND'
            WHEN 'NEW JERSEY'           THEN 'MIDDLE ATLANTIC'
            WHEN 'NEW YORK'             THEN 'MIDDLE ATLANTIC'
            WHEN 'PENNSYLVANIA'         THEN 'MIDDLE ATLANTIC'
            WHEN 'ILLINOIS'             THEN 'EAST NORTH CENTRAL'
            WHEN 'INDIANA'              THEN 'EAST NORTH CENTRAL'
            WHEN 'MICHIGAN'             THEN 'EAST NORTH CENTRAL'
            WHEN 'OHIO'                 THEN 'EAST NORTH CENTRAL'
            WHEN 'WISCONSIN'            THEN 'EAST NORTH CENTRAL'
            WHEN 'IOWA'                 THEN 'WEST NORTH CENTRAL'
            WHEN 'KANSAS'               THEN 'WEST NORTH CENTRAL'
            WHEN 'MINNESOTA'            THEN 'WEST NORTH CENTRAL'
            WHEN 'MISSOURI'             THEN 'WEST NORTH CENTRAL'
            WHEN 'NEBRASKA'             THEN 'WEST NORTH CENTRAL'
            WHEN 'NORTH DAKOTA'         THEN 'WEST NORTH CENTRAL'
            WHEN 'SOUTH DAKOTA'         THEN 'WEST NORTH CENTRAL'
            WHEN 'DELAWARE'             THEN 'SOUTH ATLANTIC'
            WHEN 'FLORIDA'              THEN 'SOUTH ATLANTIC'
            WHEN 'GEORGIA'              THEN 'SOUTH ATLANTIC'
            WHEN 'MARYLAND'             THEN 'SOUTH ATLANTIC'
            WHEN 'NORTH CAROLINA'       THEN 'SOUTH ATLANTIC'
            WHEN 'SOUTH CAROLINA'       THEN 'SOUTH ATLANTIC'
            WHEN 'VIRGINIA'             THEN 'SOUTH ATLANTIC'
            WHEN 'WEST VIRGINIA'        THEN 'SOUTH ATLANTIC'
            WHEN 'DISTRICT OF COLUMBIA' THEN 'SOUTH ATLANTIC'
            WHEN 'ALABAMA'              THEN 'EAST SOUTH CENTRAL'
            WHEN 'KENTUCKY'             THEN 'EAST SOUTH CENTRAL'
            WHEN 'MISSISSIPPI'          THEN 'EAST SOUTH CENTRAL'
            WHEN 'TENNESSEE'            THEN 'EAST SOUTH CENTRAL'
            WHEN 'ARKANSAS'             THEN 'WEST SOUTH CENTRAL'
            WHEN 'LOUISIANA'            THEN 'WEST SOUTH CENTRAL'
            WHEN 'OKLAHOMA'             THEN 'WEST SOUTH CENTRAL'
            WHEN 'TEXAS'                THEN 'WEST SOUTH CENTRAL'
            WHEN 'ARIZONA'              THEN 'MOUNTAIN'
            WHEN 'COLORADO'             THEN 'MOUNTAIN'
            WHEN 'IDAHO'                THEN 'MOUNTAIN'
            WHEN 'MONTANA'              THEN 'MOUNTAIN'
            WHEN 'NEVADA'               THEN 'MOUNTAIN'
            WHEN 'NEW MEXICO'           THEN 'MOUNTAIN'
            WHEN 'UTAH'                 THEN 'MOUNTAIN'
            WHEN 'WYOMING'              THEN 'MOUNTAIN'
            WHEN 'ALASKA'               THEN 'PACIFIC'
            WHEN 'CALIFORNIA'           THEN 'PACIFIC'
            WHEN 'HAWAII'               THEN 'PACIFIC'
            WHEN 'OREGON'               THEN 'PACIFIC'
            WHEN 'WASHINGTON'           THEN 'PACIFIC'
            ELSE NULL
        END                                                                         AS region_name,

        -- Time fields
        CAST(year AS INT64)                                                         AS year,
        CAST(week AS INT64)                                                         AS week,

        -- Disease
        REPLACE(
    		REGEXP_REPLACE(UPPER(TRIM(label)), r'\s+', ' '),
    		'SALMONELLAPARATYPHI',
    		'SALMONELLA PARATYPHI'
		)								    AS disease_name,


        -- Measures
        COALESCE(CAST(m1 AS FLOAT64), 0)                                                         AS current_week_cases,
        COALESCE(CAST(m2 AS FLOAT64), 0)                                                         AS previous_52_weeks_max,
        COALESCE(CAST(m3 AS FLOAT64), 0)                                                         AS cumulative_ytd_cases,
        COALESCE(CAST(m4 AS FLOAT64), 0)                                                         AS previous_year_cumulative,

        -- Flags
        m1_flag                                                                     AS current_week_cases_flag,
        m2_flag                                                                     AS previous_52_weeks_max_flag,
        m3_flag                                                                     AS cumulative_ytd_cases_flag,
        m4_flag                                                                     AS previous_year_cumulative_flag

    FROM source
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY state_name, year, week, disease_name
            ORDER BY sort_order_masked
        ) AS row_num
    FROM cleaned
    WHERE region_name IS NOT NULL
    AND disease_name NOT LIKE '%, TOTAL'
)

SELECT * EXCEPT (row_num)
FROM deduped
WHERE row_num = 1