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

,new_covid_tests_with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,COALESCE(tests - LAG(tests) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS tests
        ,COALESCE(vaccines - LAG(vaccines) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS vaccines
        ,COALESCE(people_vaccinated - LAG(people_vaccinated) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS people_vaccinated
        ,COALESCE(people_fully_vaccinated - LAG(people_fully_vaccinated) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS people_fully_vaccinated
    FROM covid_tests
)

SELECT
    ROW_NUMBER() OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS row_num
    ,*
FROM new_covid_tests_with_id
QUALIFY row_num = 1