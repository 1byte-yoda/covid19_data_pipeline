WITH covid_tests AS (
    SELECT DISTINCT
        location_id,
        {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_id,
        t.tests,
        t.vaccines,
        t.people_vaccinated,
        t.people_fully_vaccinated
    FROM {{ ref('cleansed_covid_datahub') }} AS t
)

SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
FROM covid_tests