WITH covid_cases AS (
    SELECT
        cl.location_id,
        t.confirmed,
        t.deaths,
        t.recovered,
        t.active,
        t.incident_rate,
        t.case_fatality_ratio,
        t.last_update
    FROM {{ ref('cleansed_github_csse_daily') }} AS t
    JOIN {{ ref('cleansed_location') }} AS cl ON cl.id = t.id
    ),

    covid_cases_with_date_id AS (
SELECT DISTINCT
    t.location_id,
    t.confirmed,
    t.deaths,
    t.recovered,
    t.active,
    t.incident_rate,
    t.case_fatality_ratio,
    last_update,
    strftime(last_update, '%Y%m%d')::INT AS date_id
FROM covid_cases AS t
    ),

    covid_cases_with_deltas AS (
    SELECT
        *,
        COALESCE(confirmed - LAG(confirmed) OVER (PARTITION BY location_id ORDER BY date_id), confirmed) AS daily_confirmed,
        COALESCE(deaths - LAG(deaths) OVER (PARTITION BY location_id ORDER BY date_id), deaths) AS daily_deaths,
        COALESCE(recovered - LAG(recovered) OVER (PARTITION BY location_id ORDER BY date_id), recovered) AS daily_recovered,
        COALESCE(active - LAG(active) OVER (PARTITION BY location_id ORDER BY date_id), active) AS daily_active
    FROM covid_cases_with_date_id
    ),

    covid_cases_with_sk AS (
        SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
        FROM covid_cases_with_deltas
    )

SELECT DISTINCT covid_id,
                FIRST_VALUE(confirmed) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS confirmed,
        FIRST_VALUE(deaths) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS deaths,
        FIRST_VALUE(recovered) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS recovered,
        FIRST_VALUE(active) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS active,
        FIRST_VALUE(incident_rate) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS incident_rate,
        FIRST_VALUE(case_fatality_ratio) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS case_fatality_ratio,
        FIRST_VALUE(last_update) OVER(PARTITION BY covid_id ORDER BY last_update DESC) AS last_update,
        * EXCLUDE (covid_id, confirmed, deaths, recovered, active, incident_rate, case_fatality_ratio, last_update)
FROM covid_cases_with_sk
