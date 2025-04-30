WITH covid_hosp AS (
    SELECT DISTINCT
        location_id,
        strftime(date, '%Y%m%d')::INT AS date_id,
            t.hosp,
        t.icu,
        t.vent
    FROM {{ ref('cleansed_covid_datahub') }} AS t
    ),

    max_values AS (
SELECT DISTINCT
    location_id,
    date_id,
    MAX(hosp) OVER (PARTITION BY location_id ORDER BY date_id) AS hosp,
    MAX(icu) OVER (PARTITION BY location_id ORDER BY date_id) AS icu,
    MAX(vent) OVER (PARTITION BY location_id ORDER BY date_id) AS vent
FROM covid_hosp
    ),

    daily_deltas AS (
SELECT
    *,
    COALESCE(hosp - LAG(hosp) OVER (PARTITION BY location_id ORDER BY date_id), hosp) AS daily_hosp,
    COALESCE(icu - LAG(icu) OVER (PARTITION BY location_id ORDER BY date_id), icu) AS daily_icu,
    COALESCE(vent - LAG(vent) OVER (PARTITION BY location_id ORDER BY date_id), vent) AS daily_vent
FROM max_values
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id,
    *
FROM daily_deltas
