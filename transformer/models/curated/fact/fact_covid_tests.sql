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
WITH cumulative_fixed AS (
    SELECT
        location_id
        ,strftime(date::DATE,'%Y%m%d')::INT AS date_id
        ,date

        -- Fix decreasing cumulative tests
        ,max(tests) OVER (
            PARTITION BY location_id
            ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS fixed_tests
        ,max(vaccines) OVER (
            PARTITION BY location_id
            ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS fixed_vaccines
        ,max(people_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS fixed_people_vaccinated
        ,max(people_fully_vaccinated) OVER (
            PARTITION BY location_id
            ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS fixed_people_fully_vaccinated
    FROM {{ ref('cleansed_covid_datahub') }}
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND date >= '{{ var('min_date') }}' AND date <= '{{ var('max_date') }}'
        {% endif %}
)

,daily_calculations AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,date
        ,fixed_tests
        ,fixed_vaccines
        ,fixed_people_vaccinated
        ,fixed_people_fully_vaccinated

        -- Deriving daily new cases
        ,fixed_tests - lag(fixed_tests,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS daily_new_tests
        ,fixed_vaccines - lag(fixed_vaccines,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS daily_new_vaccines
        ,fixed_people_vaccinated - lag(fixed_people_vaccinated,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS daily_new_people_vaccinated
        ,fixed_people_fully_vaccinated - lag(fixed_people_fully_vaccinated,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS daily_new_people_fully_vaccinated

    FROM cumulative_fixed
)

SELECT
    covid_id
    ,date_id
    ,location_id
    ,row_number() OVER (
        PARTITION BY covid_id
        ORDER BY date DESC
    ) AS row_num
    ,CASE WHEN daily_new_tests < 0 THEN 0 ELSE daily_new_tests END AS tests
    ,CASE WHEN daily_new_vaccines < 0 THEN 0 ELSE daily_new_vaccines END AS vaccines
    ,CASE WHEN daily_new_people_vaccinated < 0 THEN 0 ELSE daily_new_people_vaccinated END AS people_vaccinated
    ,CASE WHEN daily_new_people_fully_vaccinated < 0 THEN 0 ELSE daily_new_people_fully_vaccinated END AS people_fully_vaccinated
    ,now() AT TIME ZONE 'UTC' AS inserted_at
FROM daily_calculations
QUALIFY row_num = 1
