WITH covid_tests AS (
    SELECT DISTINCT
        location_id,
        strftime(date, '%Y%m%d')::INT AS date_id,
            t.tests,
        t.vaccines,
        t.people_vaccinated,
        t.people_fully_vaccinated
    FROM {{ ref('cleansed_covid_datahub') }} AS t
    ),

    max_values AS (
SELECT DISTINCT
    location_id,
    date_id,
    MAX(tests) OVER (PARTITION BY location_id ORDER BY date_id) AS tests,
    MAX(vaccines) OVER (PARTITION BY location_id ORDER BY date_id) AS vaccines,
    MAX(people_vaccinated) OVER (PARTITION BY location_id ORDER BY date_id) AS people_vaccinated,
    MAX(people_fully_vaccinated) OVER (PARTITION BY location_id ORDER BY date_id) AS people_fully_vaccinated
FROM covid_tests
    ),

    daily_deltas AS (
SELECT
    * EXCLUDE (tests, vaccines, people_vaccinated, people_fully_vaccinated),
    tests AS cum_tests,
    vaccines AS cum_vaccines,
    people_vaccinated AS cum_people_vaccinated,
    people_fully_vaccinated AS cum_people_fully_vaccinated,
    COALESCE(tests - LAG(tests) OVER (PARTITION BY location_id ORDER BY date_id), tests) AS tests,
    COALESCE(vaccines - LAG(vaccines) OVER (PARTITION BY location_id ORDER BY date_id), vaccines) AS vaccines,
    COALESCE(people_vaccinated - LAG(people_vaccinated) OVER (PARTITION BY location_id ORDER BY date_id), people_vaccinated) AS people_vaccinated,
    COALESCE(people_fully_vaccinated - LAG(people_fully_vaccinated) OVER (PARTITION BY location_id ORDER BY date_id), people_fully_vaccinated) AS people_fully_vaccinated,

FROM max_values
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id,
    *
FROM daily_deltas
