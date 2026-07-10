{{ config(
    materialized='table',
    schema='cdc_dims'
) }}

WITH location_base AS (
    SELECT DISTINCT
        state_name,
        region_name
    FROM {{ ref('stg_nndss_weekly') }}
    WHERE state_name IS NOT NULL
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['state_name']) }}  AS location_id,
        state_name,

        -- State Abbreviation
        CASE state_name
            WHEN 'ALABAMA'              THEN 'AL'
            WHEN 'ALASKA'               THEN 'AK'
            WHEN 'ARIZONA'              THEN 'AZ'
            WHEN 'ARKANSAS'             THEN 'AR'
            WHEN 'CALIFORNIA'           THEN 'CA'
            WHEN 'COLORADO'             THEN 'CO'
            WHEN 'CONNECTICUT'          THEN 'CT'
	    WHEN 'DISTRICT OF COLUMBIA' THEN 'DC'
            WHEN 'DELAWARE'             THEN 'DE'
            WHEN 'FLORIDA'              THEN 'FL'
            WHEN 'GEORGIA'              THEN 'GA'
            WHEN 'HAWAII'               THEN 'HI'
            WHEN 'IDAHO'                THEN 'ID'
            WHEN 'ILLINOIS'             THEN 'IL'
            WHEN 'INDIANA'              THEN 'IN'
            WHEN 'IOWA'                 THEN 'IA'
            WHEN 'KANSAS'               THEN 'KS'
            WHEN 'KENTUCKY'             THEN 'KY'
            WHEN 'LOUISIANA'            THEN 'LA'
            WHEN 'MAINE'                THEN 'ME'
            WHEN 'MARYLAND'             THEN 'MD'
            WHEN 'MASSACHUSETTS'        THEN 'MA'
            WHEN 'MICHIGAN'             THEN 'MI'
            WHEN 'MINNESOTA'            THEN 'MN'
            WHEN 'MISSISSIPPI'          THEN 'MS'
            WHEN 'MISSOURI'             THEN 'MO'
            WHEN 'MONTANA'              THEN 'MT'
            WHEN 'NEBRASKA'             THEN 'NE'
            WHEN 'NEVADA'               THEN 'NV'
            WHEN 'NEW HAMPSHIRE'        THEN 'NH'
            WHEN 'NEW JERSEY'           THEN 'NJ'
            WHEN 'NEW MEXICO'           THEN 'NM'
            WHEN 'NEW YORK'             THEN 'NY'
            WHEN 'NORTH CAROLINA'       THEN 'NC'
            WHEN 'NORTH DAKOTA'         THEN 'ND'
            WHEN 'OHIO'                 THEN 'OH'
            WHEN 'OKLAHOMA'             THEN 'OK'
            WHEN 'OREGON'               THEN 'OR'
            WHEN 'PENNSYLVANIA'         THEN 'PA'
            WHEN 'RHODE ISLAND'         THEN 'RI'
            WHEN 'SOUTH CAROLINA'       THEN 'SC'
            WHEN 'SOUTH DAKOTA'         THEN 'SD'
            WHEN 'TENNESSEE'            THEN 'TN'
            WHEN 'TEXAS'                THEN 'TX'
            WHEN 'UTAH'                 THEN 'UT'
            WHEN 'VERMONT'              THEN 'VT'
            WHEN 'VIRGINIA'             THEN 'VA'
            WHEN 'WASHINGTON'           THEN 'DC'
            WHEN 'WEST VIRGINIA'        THEN 'WV'
            WHEN 'WISCONSIN'            THEN 'WI'
            WHEN 'WYOMING'              THEN 'WY'
            ELSE NULL
        END AS state_abbreviation,

        region_name,

	CASE region_name
            WHEN 'NEW ENGLAND'        THEN 'NORTHEAST'
            WHEN 'MIDDLE ATLANTIC'    THEN 'NORTHEAST'
            WHEN 'EAST NORTH CENTRAL' THEN 'MIDWEST'
            WHEN 'WEST NORTH CENTRAL' THEN 'MIDWEST'
            WHEN 'SOUTH ATLANTIC'     THEN 'SOUTH'
            WHEN 'EAST SOUTH CENTRAL' THEN 'SOUTH'
            WHEN 'WEST SOUTH CENTRAL' THEN 'SOUTH'
            WHEN 'MOUNTAIN'           THEN 'WEST'
            WHEN 'PACIFIC'            THEN 'WEST'
            ELSE NULL
        END AS census_division

    FROM location_base
)

SELECT * FROM enriched