{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

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
