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

,daily_covid_hosp_with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,location_id
        ,date_id
        ,COALESCE(hosp - LAG(hosp) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS hosp
        ,COALESCE(icu - LAG(icu) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS icu
        ,COALESCE(vent - LAG(vent) OVER(PARTITION BY location_id ORDER BY date_id), 0) AS vent
    FROM covid_hosp
)


SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS row_num
FROM daily_covid_hosp_with_id
QUALIFY row_num = 1


