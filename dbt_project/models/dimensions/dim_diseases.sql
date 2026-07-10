{% set disease_modifiers = [
    'CONFIRMED',
    'PROBABLE',
    'SUSPECT',
    'INFANTS',
    'INFANT',
    'PEDIATRIC',
    'CONGENITAL SYNDROME',
    'NON-CONGENITAL',
    'CONGENITAL',
    'FOODBORNE',
    'WATERBORNE',
    'ACUTE',
    'CHRONIC',
    'ALL SEROGROUPS',
    'OTHER SEROGROUPS',
    'SEROGROUPS ACWY',
    'SEROGROUP B',
    'UNKNOWN SEROGROUP',
    'UNKNOWN SEROTYPE',
    'NONPARALYTIC',
    'PARALYTIC',
    'AGE <5 YEARS',
    'ALL AGES',
    'CLINICAL',
    'SCREENING',
    'HUMAN',
    'ANIMAL',
    'PRIMARY AND SECONDARY',
    'PERINATAL INFECTION',
    'PERINATAL',
    'OTHER \\(WOUND & UNSPECIFIED\\)',
    'NON-B SEROTYPE',
    'NONTYPEABLE',
    'SEROTYPE B',
    'UNKNOWN SEROTYPE',
    'ALL SEROTYPES'
] %}

{{ config(
    materialized='table',
    schema='cdc_dims',
    hours_to_expiration=8760
) }}

WITH diseases_unique AS (
    SELECT DISTINCT
        disease_name
    FROM {{ ref('stg_nndss_weekly') }}
),

final_table AS (
    SELECT
	{{ dbt_utils.generate_surrogate_key(['disease_name']) }} AS disease_id,
	disease_name AS disease_name_long,

        -- 1. Extract Disease Status
        CASE
            WHEN disease_name LIKE '%CONFIRMED%' THEN 'Confirmed'
            WHEN disease_name LIKE '%PROBABLE%'  THEN 'Probable'
            WHEN disease_name LIKE '%SUSPECT%'   THEN 'Suspect'
            ELSE 'N/A'
        END AS disease_status,

        -- 2. Extract Demographic Modifier
        CASE
            WHEN disease_name LIKE '%INFANTS%'   THEN 'Infant'
            WHEN disease_name LIKE '%INFANT%'    THEN 'Infant'
            WHEN disease_name LIKE '%PEDIATRIC%' THEN 'Pediatric'
            WHEN disease_name LIKE '%<5 YEARS%'  THEN 'Under 5 Years'
            WHEN disease_name LIKE '%ALL AGES%'  THEN 'All Ages'
            WHEN disease_name LIKE '%OTHER%'     THEN 'Other'
            ELSE 'N/A'
        END AS target_demographic,

        -- 3. Extract Disease Category
        CASE
            WHEN disease_name LIKE '%FOODBORNE%'           THEN 'Foodborne'
            WHEN disease_name LIKE '%WATERBORNE%'          THEN 'Waterborne'
            WHEN disease_name LIKE '%ARBOVIRAL%'           THEN 'Arboviral/Vector-borne'
            WHEN disease_name LIKE '%PERINATAL%'           THEN 'Perinatal'
            WHEN disease_name LIKE '%NON-CONGENITAL%'      THEN 'Non-Congenital'
            WHEN disease_name LIKE '%CONGENITAL%'          THEN 'Congenital'
            WHEN disease_name LIKE '%PRIMARY AND SECONDARY%' THEN 'Primary/Secondary'
            ELSE 'N/A'
        END AS disease_category,

        -- 4. Extract Severity/Stage
        CASE
            WHEN disease_name LIKE '%ACUTE%'   THEN 'Acute'
            WHEN disease_name LIKE '%CHRONIC%' THEN 'Chronic'
            ELSE 'N/A'
        END AS severity_or_stage,

        -- 5. Extract Pathogen Subtype
        CASE
            WHEN disease_name LIKE '%SEROGROUP B%'       THEN 'Serogroup B'
            WHEN disease_name LIKE '%SEROGROUPS ACWY%'   THEN 'Serogroups ACWY'
            WHEN disease_name LIKE '%OTHER SEROGROUPS%'  THEN 'Other Serogroups'
            WHEN disease_name LIKE '%ALL SEROGROUPS%'    THEN 'All Serogroups'
            WHEN disease_name LIKE '%UNKNOWN SEROGROUP%' THEN 'Unknown Serogroup'
            WHEN disease_name LIKE '%NON-B SEROTYPE%'    THEN 'Non-B Serotype'
            WHEN disease_name LIKE '%NONTYPEABLE%'       THEN 'Nontypeable'
            WHEN disease_name LIKE '%SEROTYPE B%'        THEN 'Serotype B'
            WHEN disease_name LIKE '%UNKNOWN SEROTYPE%'  THEN 'Unknown Serotype'
            WHEN disease_name LIKE '%ALL SEROTYPES%'     THEN 'All Serotypes'
            ELSE 'N/A'
        END AS pathogen_subtype,

        -- 6. Extract Detection Method
        CASE
            WHEN disease_name LIKE '%SCREENING%' THEN 'Screening'
            WHEN disease_name LIKE '%CLINICAL%'  THEN 'Clinical'
            ELSE 'N/A'
        END AS detection_method,

        -- 7. Extract Disease Group
        CASE
            WHEN disease_name LIKE 'ARBOVIRAL DISEASES%'          THEN 'Arboviral Diseases'
            WHEN disease_name LIKE 'EHRLICHIOSIS AND ANAPLASMOSIS%' THEN 'Ehrlichiosis and Anaplasmosis'
            WHEN disease_name LIKE 'HAEMOPHILUS INFLUENZAE%'      THEN 'Haemophilus Influenzae'
            WHEN disease_name LIKE 'HEPATITIS%'                   THEN 'Hepatitis'
            WHEN disease_name LIKE 'VIRAL HEMORRHAGIC FEVERS%'    THEN 'Viral Hemorrhagic Fevers'
            WHEN disease_name LIKE 'DENGUE VIRUS INFECTIONS%'     THEN 'Dengue Virus Infections'
            ELSE 'N/A'
        END AS disease_group,

        -- 8. Extract Short Disease Name
        REGEXP_REPLACE(
            TRIM(
                REGEXP_REPLACE(
                    CASE
                        WHEN disease_name LIKE 'ARBOVIRAL DISEASES, %'          THEN REPLACE(disease_name, 'ARBOVIRAL DISEASES, ', '')
                        WHEN disease_name LIKE 'EHRLICHIOSIS AND ANAPLASMOSIS, %' THEN REPLACE(disease_name, 'EHRLICHIOSIS AND ANAPLASMOSIS, ', '')
                        WHEN disease_name LIKE 'DENGUE VIRUS INFECTIONS, %'     THEN REPLACE(disease_name, 'DENGUE VIRUS INFECTIONS, ', '')
                        WHEN disease_name LIKE 'VIRAL HEMORRHAGIC FEVERS, %'    THEN REPLACE(disease_name, 'VIRAL HEMORRHAGIC FEVERS, ', '')
                        WHEN disease_name LIKE 'HAEMOPHILUS INFLUENZAE%'        THEN REPLACE(disease_name, ', INVASIVE DISEASE', '')
                        ELSE disease_name
                    END,
                    r'[,-]?\s*({{ disease_modifiers | join("|") }})',
                    ''
                )
            ),
            r'\s+', ' '
        ) AS disease_name_short

    FROM diseases_unique
    WHERE disease_name NOT LIKE '%ASSOCIATED%'
)

SELECT * FROM final_table