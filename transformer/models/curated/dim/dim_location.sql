{{ config(unique_key='location_id', incremental_strategy="delete+insert") }}

WITH location_combined AS (
    SELECT
        COALESCE(cl.location_id, ccd.location_id) AS location_id,
        COALESCE(cl.country, ccd.country) AS country,
        COALESCE(cl.state, ccd.state) AS state,
        COALESCE(cl.city, ccd.city) AS city,
        COALESCE(cl.administrative_area_level, ccd.administrative_area_level) AS administrative_area_level,
        COALESCE(cl.latitude, 0) AS latitude,
        COALESCE(cl.longitude, 0) AS longitude,
        COALESCE(ccd.population, 0) AS population,
        cl.last_update -- used for incremental filter
    FROM {{ ref('cleansed_location') }} AS cl
    FULL OUTER JOIN {{ ref('cleansed_covid_datahub') }} AS ccd
        ON cl.location_id = ccd.location_id
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND cl.last_update >= '{{ var('min_date') }}'
            AND cl.last_update <= '{{ var('max_date') }}'
        {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY last_update DESC) AS row_num
    FROM location_combined
)

SELECT DISTINCT
    location_id,
    {{ title_case('country') }} AS country,
    {{ title_case('state') }} AS state,
    {{ title_case('city') }} AS city,
    administrative_area_level,
    latitude,
    longitude,
    population
FROM deduped
WHERE row_num = 1
