{{ config(unique_key='id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model standardizes and enriches location-related metadata from
--     the raw COVID-19 location dataset by:
--     1. Cleaning and normalizing location fields including country, state, and city.
--     2. Resolving inconsistent latitude/longitude field names and removing invalid coordinates.
--     3. Generating a consistent `combined_key` based on administrative granularity.
--     4. Inferring `administrative_area_level` based on the presence of location fields.
--     5. Creating a surrogate `location_id` key for standardized location matching.
--     6. Supporting incremental updates based on `last_update` within a date range.
-- -----------------------------------------------------------------------------

WITH raw_location_cleansed AS (

    SELECT
        id
        ,combined_key

        -- Standardize Location Fields
        ,{{ standardize_country('country_region') }} AS country
        ,{{ standardize_state('province_state') }} AS state

        -- Fill Null Values
        ,COALESCE(admin2,'Unassigned') AS city
        ,COALESCE(fips::INT,-9999) AS fips

        -- Nullify longitude and latitude values for easy COALESCE
        ,NULLIF(latitude,0) AS latitude
        ,NULLIF(longitude,0) AS longitude
        ,NULLIF(lat,0) AS lat
        ,NULLIF(longx,0) AS longx

        -- Parse timestamp field
        ,{{ try_parse_timestamp('last_update') }} AS last_update
    FROM {{ ref('raw_location') }}
)

,location_with_derived_fields AS (
    SELECT
        id
        ,combined_key
        ,state
        ,city
        ,country
        ,fips
        ,last_update

        -- Lowercased location fields to be used to standardize location surrogate key
        ,LOWER(state) AS low_state
        ,LOWER(city) AS low_city
        ,LOWER(country) AS low_country

        -- Get the non-null location coordinates
        ,COALESCE(latitude,lat,0) AS latitude
        ,COALESCE(longitude,longx,0) AS longitude

        -- Derive administrative_area_level from cleansed city field
        ,CASE
            WHEN city IN ('Unassigned','Unknown') AND state IN ('Unassigned','Unknown') THEN 1
            WHEN city IN ('Unassigned','Unknown') THEN 2
            WHEN city NOT IN ('Unassigned','Unknown') AND state NOT IN ('Unassigned','Unknown') THEN 3
            ELSE -1
        END AS administrative_area_level
    FROM raw_location_cleansed
)

,final_location AS (
    SELECT
        * EXCLUDE (combined_key,low_country,low_state,low_city)

        -- Derive combined_key from the cleansed location fields
        ,CASE
            WHEN administrative_area_level = 3 THEN CONCAT(city,', ',state,', ',country)
            WHEN administrative_area_level = 2 THEN CONCAT(state,', ',country)
            ELSE country
        END AS combined_key

        -- Generate location_id from lower cased location fields
        ,{{ dbt_utils.generate_surrogate_key(['low_country', 'low_state', 'low_city', 'administrative_area_level']) }} AS location_id
    FROM location_with_derived_fields
)

SELECT
    *
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM final_location
WHERE
    1 = 1
    {% if is_incremental() %}
        AND last_update >= '{{ var('min_date') }}' AND last_update <= '{{ var('max_date') }}'
    {% endif %}
