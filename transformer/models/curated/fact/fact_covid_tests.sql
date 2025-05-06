{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model computes the **daily number of COVID-19 tests and vaccinations**
--     from cumulative totals. It:
--     1. Extracts distinct cumulative metrics from `cleansed_covid_datahub`.
--     2. Computes lag values to identify previous day's totals by location.
--     3. Derives daily deltas using conditional logic to ensure only increasing values
--        are considered (handling data corrections or missing values gracefully).
--     4. Generates a surrogate key (`covid_id`) using `location_id` and `date_id`.
--     5. Implements an incremental strategy using `min_date` and `max_date` filters.
--     6. Deduplicates using `row_number()` to return only the latest record per key.
-- -----------------------------------------------------------------------------

WITH covid_tests AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date::DATE,'%Y%m%d')::INT AS date_id
        ,t.tests
        ,t.vaccines
        ,t.people_vaccinated
        ,t.people_fully_vaccinated
        ,t.date
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
            ORDER BY date
        ) AS prev_tests
        ,lag(vaccines) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS prev_vaccines
        ,lag(people_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS prev_people_vaccinated
        ,lag(people_fully_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS prev_people_fully_vaccinated
    FROM covid_tests
)

,new_covid_tests_with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,date

        -- Automatically Correct Cumulative Values where older cases is always lower than present cases
        ,CASE WHEN prev_tests < tests THEN tests - prev_tests ELSE 0 END AS tests
        ,CASE WHEN prev_vaccines < vaccines THEN vaccines - prev_vaccines ELSE 0 END AS vaccines
        ,CASE WHEN prev_people_vaccinated < people_vaccinated THEN people_vaccinated - prev_people_vaccinated ELSE 0 END AS people_vaccinated
        ,CASE WHEN prev_people_fully_vaccinated < people_fully_vaccinated THEN people_fully_vaccinated - prev_people_fully_vaccinated ELSE 0 END
            AS people_fully_vaccinated
    FROM covid_with_prev_tests
)

SELECT
    * EXCLUDE(date)
    ,row_number() OVER (
        PARTITION BY covid_id
        ORDER BY date DESC
    ) AS row_num
    ,now() AT TIME ZONE 'UTC' AS inserted_at
FROM new_covid_tests_with_id
QUALIFY row_num = 1
