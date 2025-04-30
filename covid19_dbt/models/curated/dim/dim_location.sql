WITH location AS (
    SELECT COALESCE(cl.location_id, ccd.location_id) AS location_id,
           COALESCE(cl.country, ccd.country) AS country,
           COALESCE(cl.state, ccd.state) AS state,
           COALESCE(cl.city, ccd.city) AS city,
           MAX(cl.latitude) AS latitude,
           MAX(cl.longitude) AS longitude,
           COALESCE(cl.administrative_area_level, ccd.administrative_area_level) AS administrative_area_level,
           ccd.population
    FROM {{ ref('cleansed_location') }} AS cl
    FULL OUTER JOIN {{ ref('cleansed_covid_datahub') }} AS ccd ON ccd.location_id = cl.location_id
    GROUP BY ALL
)

SELECT *
FROM location