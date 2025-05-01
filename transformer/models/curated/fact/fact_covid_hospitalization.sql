WITH covid_hosp AS (
    SELECT DISTINCT
        t.location_id
        ,strftime(t.date,'%Y%m%d')::INT AS date_id
        ,t.hosp
        ,t.icu
        ,t.vent
    FROM {{ ref('cleansed_covid_datahub') }} AS t
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

,daily_deltas AS (
    SELECT
        * EXCLUDE (hosp,icu,vent)
        ,hosp AS cum_hosp
        ,icu AS cum_icu
        ,vent AS cum_vent
        ,coalesce(hosp - lag(hosp) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ),hosp) AS hosp
        ,coalesce(icu - lag(icu) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ),icu) AS icu
        ,coalesce(vent - lag(vent) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ),vent) AS vent
    FROM max_values
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
    ,*
FROM daily_deltas
