{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model processes COVID-19 hospitalization data by:
--     1. Selecting distinct daily records of hospitalization, ICU, and ventilator usage.
--     2. Generating a surrogate `covid_id` using `location_id` and `date_id`.
--     3. Filling null values with zeros for numerical stability.
--     4. Deduplicating to retain the latest record per location and date.
--     5. Supporting incremental strategy using `min_date` and `max_date` filters.
-- -----------------------------------------------------------------------------

WITH covid_hosp AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date::DATE,'%Y%m%d')::INT AS date_id
        ,t.hosp
        ,t.icu
        ,t.vent
    FROM {{ ref('cleansed_covid_datahub') }} AS t
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND t.date >= '{{ var('min_date') }}' AND t.date <= '{{ var('max_date') }}'
        {% endif %}
)

,covid_hosp_with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,coalesce(hosp,0) AS hosp
        ,coalesce(icu,0) AS icu
        ,coalesce(vent,0) AS vent
    FROM covid_hosp
)

SELECT
    *
    ,row_number() OVER (
        PARTITION BY covid_id
        ORDER BY date_id DESC
    ) AS row_num
    ,now() AT TIME ZONE 'UTC' AS inserted_at
FROM covid_hosp_with_id
QUALIFY row_num = 1
