WITH location AS (
    SELECT DISTINCT COALESCE(cl.location_id, ccd.location_id) AS location_id,
           COALESCE(cl.country, ccd.country) AS country,
           COALESCE(cl.state, ccd.state) AS state,
           COALESCE(cl.city, ccd.city) AS city,
           cl.latitude,
           cl.longitude,
           COALESCE(cl.administrative_area_level, ccd.administrative_area_level) AS administrative_area_level,
           ccd.population
    FROM {{ ref('cleansed_location') }} AS cl
    FULL OUTER JOIN {{ ref('cleansed_covid_datahub') }} AS ccd ON ccd.location_id = cl.location_id
)

SELECT DISTINCT location_id,
    {{ title_case('country' )}} AS country,
    {{ title_case('state' )}} AS state,
    {{ title_case('city' )}} AS city,
    administrative_area_level,
    MAX(latitude) OVER(PARTITION BY location_id ORDER BY location_id) AS latitude,
    MAX(longitude) OVER(PARTITION BY location_id ORDER BY location_id) AS longitude,
    MAX(population) OVER(PARTITION BY location_id ORDER BY location_id) AS population

FROM location