{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

WITH covid_tests AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date::DATE,'%Y%m%d')::INT AS date_id
        ,t.tests
        ,t.vaccines
        ,t.people_vaccinated
        ,t.people_fully_vaccinated
    FROM {{ ref('cleansed_covid_datahub') }} AS t
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND t.date >= '{{ var('min_date') }}' AND t.date <= '{{ var('max_date') }}'
        {% endif %}
)

,covid_with_prev_tests AS (
    SELECT
        *
        ,lag(tests) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_tests
        ,lag(vaccines) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_vaccines
        ,lag(people_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_people_vaccinated
        ,lag(people_fully_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_people_fully_vaccinated
    FROM covid_tests
)

,new_covid_tests_with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,CASE WHEN prev_tests < tests THEN tests - prev_tests ELSE 0 END AS tests
        ,CASE WHEN prev_vaccines < vaccines THEN vaccines - prev_vaccines ELSE 0 END AS vaccines
        ,CASE WHEN prev_people_vaccinated < people_vaccinated THEN people_vaccinated - prev_people_vaccinated ELSE 0 END AS people_vaccinated
        ,CASE WHEN prev_people_fully_vaccinated < people_fully_vaccinated THEN people_fully_vaccinated - prev_people_fully_vaccinated ELSE 0 END
            AS people_fully_vaccinated
    FROM covid_with_prev_tests
)

SELECT
    *
    ,row_number() OVER (
        PARTITION BY covid_id
        ORDER BY date_id DESC
    ) AS row_num
    ,now() AT TIME ZONE 'UTC' AS inserted_at
FROM new_covid_tests_with_id
QUALIFY row_num = 1
