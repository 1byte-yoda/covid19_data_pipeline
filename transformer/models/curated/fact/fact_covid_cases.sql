{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model processes COVID-19 case data at the daily level by:
--     1. Joining cleansed case records with location metadata.
--     2. Generating a surrogate `covid_id` from `location_id` and `date_id`.
--     3. Deduplicating records based on the most recent data per location-date.
--     4. Computing daily deltas for confirmed, deaths, recovered, and active cases.
--     5. Recalculating incident rate and case fatality ratio for accuracy.
--     6. Supporting incremental loads via `min_date` and `max_date`.
-- -----------------------------------------------------------------------------

WITH cumulative_fixed AS (
    SELECT
        cl.location_id
        ,strftime(t.last_update::DATE,'%Y%m%d')::INT AS date_id
        ,t.last_update AS date

        -- Ensure cumulative confirmed, deaths, recovered, active don't decrease
        ,t.incident_rate
        ,t.case_fatality_ratio
        ,max(t.confirmed) OVER (
            PARTITION BY cl.location_id
            ORDER BY t.last_update
        ) AS fixed_confirmed
        ,max(t.deaths) OVER (
            PARTITION BY cl.location_id
            ORDER BY t.last_update
        ) AS fixed_deaths

        ,max(t.recovered) OVER (
            PARTITION BY cl.location_id
            ORDER BY t.last_update
        ) AS fixed_recovered
        ,max(t.active) OVER (
            PARTITION BY cl.location_id
            ORDER BY t.last_update
        ) AS fixed_active
        ,t.confirmed * 100000 / nullif(t.incident_rate,0) AS population

    FROM {{ ref('cleansed_github_csse_daily') }} AS t
    INNER JOIN {{ ref('cleansed_location') }} AS cl ON t.id = cl.id
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND t.last_update >= '{{ var('min_date') }}' AND t.last_update <= '{{ var('max_date') }}'
        {% endif %}
)

,calc_daily_new_cases AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,date
        ,population

        -- Calculate daily new cases
        ,fixed_confirmed - lag(fixed_confirmed,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS raw_daily_confirmed
        ,fixed_deaths - lag(fixed_deaths,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS raw_daily_deaths
        ,fixed_recovered - lag(fixed_recovered,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS raw_daily_recovered
        ,fixed_active - lag(fixed_active,1,0) OVER (
            PARTITION BY location_id
            ORDER BY date
        ) AS raw_daily_active

    FROM cumulative_fixed
)

,final_daily_cases AS (
    SELECT
        covid_id
        ,date_id
        ,location_id
        ,population

        -- Fix negatives by replacing with zero
        ,CASE WHEN raw_daily_confirmed < 0 THEN 0 ELSE raw_daily_confirmed END AS confirmed
        ,CASE WHEN raw_daily_deaths < 0 THEN 0 ELSE raw_daily_deaths END AS deaths
        ,CASE WHEN raw_daily_recovered < 0 THEN 0 ELSE raw_daily_recovered END AS recovered
        ,CASE WHEN raw_daily_active < 0 THEN 0 ELSE raw_daily_active END AS active

    FROM calc_daily_new_cases
)

,recalculated_metrics AS (
    SELECT
        * EXCLUDE (population)
        ,row_number() OVER (
            PARTITION BY covid_id
            ORDER BY date_id DESC
        ) AS row_num
        ,coalesce(confirmed / nullif(population,0) * 100000,0) AS incident_rate
        ,CASE WHEN confirmed = 0 THEN 0 ELSE deaths / confirmed END AS case_fatality_ratio
    FROM final_daily_cases
    QUALIFY row_num = 1
)

SELECT
    *
    ,now() AT TIME ZONE 'UTC' AS inserted_at
FROM recalculated_metrics
