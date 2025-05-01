{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

WITH covid_tests AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date,'%Y%m%d')::INT AS date_id
        ,t.tests
        ,t.vaccines
        ,t.people_vaccinated
        ,t.people_fully_vaccinated
    FROM {{ ref('cleansed_covid_datahub') }} AS t
    WHERE 1 = 1
    {% if is_incremental() %}
        AND t.date >= '{{ var('min_date') }}' AND t.date <= '{{ var('max_date') }}'
    {% endif %}
)

,max_values AS (
    SELECT DISTINCT
        location_id
        ,date_id
        ,max(tests) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS tests
        ,max(vaccines) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS vaccines
        ,max(people_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS people_vaccinated
        ,max(people_fully_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS people_fully_vaccinated
    FROM covid_tests
)

,daily_deltas AS (
    SELECT
        * EXCLUDE (tests,vaccines,people_vaccinated,people_fully_vaccinated)
        ,tests AS cum_tests
        ,vaccines AS cum_vaccines
        ,people_vaccinated AS cum_people_vaccinated
        ,people_fully_vaccinated AS cum_people_fully_vaccinated
        ,coalesce(tests - lag(tests) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ),tests) AS tests
        ,coalesce(vaccines - lag(vaccines) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ),vaccines) AS vaccines
        ,coalesce(
            people_vaccinated - lag(people_vaccinated) OVER (
                PARTITION BY location_id
                ORDER BY date_id
            ),people_vaccinated
        ) AS people_vaccinated
        ,coalesce(
            people_fully_vaccinated - lag(people_fully_vaccinated) OVER (
                PARTITION BY location_id
                ORDER BY date_id
            ),people_fully_vaccinated
        ) AS people_fully_vaccinated

    FROM max_values
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
    ,*
FROM daily_deltas
