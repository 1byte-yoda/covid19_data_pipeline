WITH raw_location_cleaned AS (
    SELECT id,
           REPLACE(combined_key, 'Unknown', 'Unassigned') AS combined_key,
           COALESCE(REPLACE(province_state, 'Unknown', 'Unassigned'), 'Unassigned') AS province_state,
           COALESCE(REPLACE(country_region, 'Unknown', 'Unassigned'), 'Unassigned') AS country_region,
           COALESCE(REPLACE(admin2, 'Unknown', 'Unassigned'), 'Unassigned') AS state,
           CASE WHEN latitude = 0 THEN NULL ELSE latitude END AS latitude,
           CASE WHEN longitude = 0 THEN NULL ELSE longitude END AS longitude,
           CASE WHEN lat = 0 THEN NULL ELSE lat END AS lat,
           CASE WHEN longx = 0 THEN NULL ELSE longx END AS longx,
           COALESCE(fips::INT, -9999) AS fips
    FROM {{ ref('raw_location') }}
), _cleansed_location AS (
    SELECT id AS covid_id,
           TRIM(
               COALESCE(t1.combined_key, CONCAT(t1.province_state, ', ', t1.country_region), 'Unassigned'), ','
           ) AS combined_key,
           COALESCE(t1.latitude, t1.lat, rlc.latitude, 0) AS latitude,
           COALESCE(t1.longitude, t1.longx, rlc.longitude, 0) AS longitude,
           t1.province_state,
           t1.country_region,
           t1.fips,
           t1.state
    FROM raw_location_cleaned AS t1
    LEFT JOIN {{ ref('raw_googlemaps_coordinates.sql') }} AS rlc ON rlc.combined_key = t1.combined_key
)

SELECT {{ dbt_utils.generate_surrogate_key(
            ['province_state', 'country_region', 'state']
        ) }} AS location_id,
       *
FROM _cleansed_location
