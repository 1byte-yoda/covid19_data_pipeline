WITH covid_cases AS (
    SELECT DISTINCT
        cl.location_id,
        cl.country,
        cl.state,
        cl.city,
        cl.administrative_area_level,
        strftime(t.last_update, '%Y-%m-%d') AS last_update_date,
        t.confirmed,
        t.deaths,
        t.recovered,
        t.active,
        t.incident_rate,
        t.case_fatality_ratio
    FROM {{ ref('cleansed_github_csse_daily') }} AS t
    JOIN {{ ref('cleansed_location') }} AS cl ON cl.id = t.id
), covid_cases_with_date_id AS (
    SELECT DISTINCT
        t.location_id,
        country,
        state,
        city,
        administrative_area_level,
        {{ dbt_utils.generate_surrogate_key(['last_update_date']) }} AS date_id,
        t.confirmed,
        t.deaths,
        t.recovered,
        t.active,
        t.incident_rate,
        t.case_fatality_ratio
    FROM covid_cases AS t
)

SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
FROM covid_cases_with_date_id