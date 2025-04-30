WITH raw_location_cleansed AS (
    SELECT
        id
        ,combined_key
        ,COALESCE({{ standardize_country('country_region') }},'Unassigned') AS country
        ,COALESCE({{ standardize_state('province_state') }},'Unassigned') AS state
        ,COALESCE(admin2,'Unassigned') AS city
        ,COALESCE(fips::INT,-9999) AS fips
        ,NULLIF(latitude,0) AS latitude
        ,NULLIF(longitude,0) AS longitude
        ,NULLIF(lat,0) AS lat
        ,NULLIF(longx,0) AS longx
    FROM {{ ref('raw_location') }}
)

,cleansed_location AS (
    SELECT
        id
        ,combined_key
        ,state
        ,city
        ,country
        ,fips
        ,LOWER(state) AS low_state
        ,LOWER(city) AS low_city
        ,LOWER(country) AS low_country
        ,COALESCE(latitude,lat) AS latitude
        ,COALESCE(longitude,longx) AS longitude
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
        {{ dbt_utils.generate_surrogate_key(['low_country', 'low_state', 'low_city', 'administrative_area_level']) }} AS location_id
        ,CASE
            WHEN administrative_area_level = 3 THEN CONCAT(city,', ',state,', ',country)
            WHEN administrative_area_level = 2 THEN CONCAT(state,', ',country)
            ELSE country
        END AS combined_key
        ,* EXCLUDE (combined_key,low_country,low_state,low_city)
    FROM cleansed_location
)

SELECT *
FROM final_location
