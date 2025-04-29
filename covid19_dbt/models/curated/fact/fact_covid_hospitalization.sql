WITH covid_hosp AS (
    SELECT DISTINCT
       location_id,
       {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_id,
        t.hosp,
        t.icu,
        t.vent
    FROM {{ ref('cleansed_covid_datahub') }} AS t
)

SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
FROM covid_hosp