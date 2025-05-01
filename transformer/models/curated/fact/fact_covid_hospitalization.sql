{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

WITH covid_hosp AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date,'%Y%m%d')::INT AS date_id
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

,max_values AS (
    SELECT DISTINCT
        location_id
        ,date_id
        ,max(hosp) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS hosp
        ,max(icu) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS icu
        ,max(vent) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS vent
    FROM covid_hosp
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
    ,*
FROM max_values
