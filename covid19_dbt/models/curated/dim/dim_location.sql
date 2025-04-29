WITH location AS (
    SELECT cl.location_id,
           cl.country,
           cl.state,
           cl.city,
           MAX(cl.latitude) AS latitude,
           MAX(cl.longitude) AS longitude,
           cl.administrative_area_level,
           ccd.population
    FROM {{ ref('cleansed_location') }} AS cl
    LEFT JOIN {{ ref('cleansed_covid_datahub') }} AS ccd ON ccd.location_id = cl.location_id
    GROUP BY ALL
)

SELECT *
FROM location