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
    strftime(last_update, '%Y%m%d')::INT AS date_id
FROM covid_cases AS t
    ),

    covid_cases_with_sk AS (
        SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
        FROM covid_cases_with_date_id
    ),

    unique_covid_cases AS (
    SELECT DISTINCT covid_id,
                    FIRST_VALUE(confirmed) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS confirmed,
            FIRST_VALUE(deaths) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS deaths,
            FIRST_VALUE(recovered) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS recovered,
            FIRST_VALUE(active) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS active,
            FIRST_VALUE(incident_rate) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS incident_rate,
            FIRST_VALUE(case_fatality_ratio) OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS case_fatality_ratio,
            * EXCLUDE (covid_id, confirmed, deaths, recovered, active, incident_rate, case_fatality_ratio)
    FROM covid_cases_with_sk
)


SELECT
    * EXCLUDE (confirmed, deaths, recovered, active),
    confirmed AS cum_confirmed,
    deaths AS cum_deaths,
    recovered AS cum_recovered,
    active AS cum_active,
    COALESCE(confirmed - LAG(confirmed) OVER (PARTITION BY location_id ORDER BY date_id), confirmed) AS confirmed,
    COALESCE(deaths - LAG(deaths) OVER (PARTITION BY location_id ORDER BY date_id), deaths) AS deaths,
    COALESCE(recovered - LAG(recovered) OVER (PARTITION BY location_id ORDER BY date_id), recovered) AS recovered,
    COALESCE(active - LAG(active) OVER (PARTITION BY location_id ORDER BY date_id), active) AS active
FROM unique_covid_cases

