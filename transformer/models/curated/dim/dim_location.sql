{{ config(unique_key='location_id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model produces a unified, deduplicated, and enriched geographic
--     location dimension for COVID-19 analytics by:
--     1. De-duplicating location records from GitHub and DataHub sources based on
--        the latest `last_update` and `date` values respectively.
--     2. Merging both sources using a full outer join on `location_id`.
--     3. Filling in missing geographic and demographic data using COALESCE.
--     4. Joining with a static country mapping source to append ISO codes and
--        continent information.
--     5. Supporting incremental loads using `min_date` and `max_date` filters.
-- -----------------------------------------------------------------------------

WITH deduped_github_covid_locations AS (
    SELECT DISTINCT
        cl.location_id
        ,cl.country
        ,cl.state
        ,cl.city
        ,cl.administrative_area_level
        ,cl.latitude
        ,cl.longitude
        ,ROW_NUMBER() OVER (
            PARTITION BY cl.location_id
            ORDER BY cl.last_update DESC
        ) AS row_num
    FROM {{ ref('cleansed_location') }} AS cl
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND cl.last_update >= '{{ var('min_date') }}'
            AND cl.last_update <= '{{ var('max_date') }}'
        {% endif %}
    QUALIFY row_num = 1
)

,deduped_datahub_covid_location AS (
    SELECT DISTINCT
        ccd.location_id
        ,ccd.country
        ,ccd.state
        ,ccd.city
        ,ccd.administrative_area_level
        ,ccd.population
        ,ccd.date
        ,ROW_NUMBER() OVER (
            PARTITION BY ccd.location_id
            ORDER BY ccd.date DESC
        ) AS row_num
    FROM {{ ref('cleansed_covid_datahub') }} AS ccd
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND ccd.date >= '{{ var('min_date') }}'
            AND ccd.date <= '{{ var('max_date') }}'
        {% endif %}
    QUALIFY row_num = 1
)

,location_combined AS (
    SELECT
        COALESCE(cl.latitude,0)::DECIMAL(12,7) AS latitude
        ,COALESCE(cl.longitude,0)::DECIMAL(12,7) AS longitude
        ,COALESCE(ccd.location_id,cl.location_id) AS location_id
        ,COALESCE(ccd.country,cl.country) AS country
        ,COALESCE(ccd.state,cl.state) AS state
        ,COALESCE(ccd.city,cl.city) AS city
        ,COALESCE(ccd.administrative_area_level,cl.administrative_area_level) AS administrative_area_level
        ,COALESCE(ccd.population,0) AS population
    FROM deduped_github_covid_locations AS cl
    FULL OUTER JOIN deduped_datahub_covid_location AS ccd
        ON cl.location_id = ccd.location_id
)

,static_country_map AS (
    SELECT
        country
        ,iso2
        ,iso3
        ,continent
    FROM {{ source('static_source', 'country_mapping') }}
)

SELECT
    lc.location_id
    ,{{ title_case('lc.country') }} AS country
    ,{{ title_case('lc.state') }} AS state
    ,{{ title_case('lc.city') }} AS city
    ,lc.administrative_area_level
    ,lc.latitude
    ,lc.longitude
    ,lc.population
    ,COALESCE(sc.continent,'Unassigned') AS continent
    ,COALESCE(sc.iso2,'Unassigned') AS iso2
    ,COALESCE(sc.iso3,'Unassigned') AS iso3
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM location_combined AS lc
LEFT JOIN static_country_map AS sc ON LOWER(sc.country) = LOWER(lc.country)
