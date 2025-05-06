{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

WITH covid_cases AS (
    SELECT
        cl.location_id
        ,t.confirmed
        ,t.deaths
        ,t.recovered
        ,t.active
        ,t.incident_rate
        ,t.case_fatality_ratio
        ,t.last_update
    FROM {{ ref('cleansed_github_csse_daily') }} AS t
    INNER JOIN {{ ref('cleansed_location') }} AS cl ON t.id = cl.id
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND t.last_update >= '{{ var('min_date') }}' AND t.last_update <= '{{ var('max_date') }}'
        {% endif %}
)

,covid_cases_with_date_id AS (
    SELECT DISTINCT
        t.location_id
        ,t.confirmed
        ,t.deaths
        ,t.recovered
        ,t.active
        ,t.incident_rate
        ,t.case_fatality_ratio
        ,strftime(t.last_update::DATE,'%Y%m%d')::INT AS date_id
    FROM covid_cases AS t
)

,covid_cases_with_sk AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
        ,*
    FROM covid_cases_with_date_id
)

,unique_covid_cases AS (
    SELECT
        covid_id
        ,location_id
        ,confirmed
        ,deaths
        ,recovered
        ,active
        ,incident_rate
        ,case_fatality_ratio
        ,date_id
        ,confirmed * 100000 / incident_rate AS population
        ,ROW_NUMBER() OVER(PARTITION BY covid_id ORDER BY date_id DESC) AS row_num
    FROM covid_cases_with_sk
    QUALIFY row_num = 1
)

,covid_with_prev_cases AS (
    SELECT
        *
        ,lag(confirmed) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_confirmed
        ,lag(deaths) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_deaths
        ,lag(recovered) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_recovered
        ,lag(active) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS prev_active
    FROM unique_covid_cases
)

,new_covid_cases AS (
    SELECT
    covid_id
    ,date_id
    ,location_id
    ,incident_rate
    ,case_fatality_ratio
    ,population
    ,CASE WHEN prev_confirmed < confirmed THEN confirmed - prev_confirmed ELSE 0 END AS confirmed
    ,CASE WHEN prev_deaths < deaths THEN deaths - prev_deaths ELSE 0 END AS deaths
    ,CASE WHEN prev_recovered < recovered THEN recovered - prev_recovered ELSE 0 END AS recovered
    ,CASE WHEN prev_active < active THEN active - prev_active ELSE 0 END AS active
    FROM covid_with_prev_cases
)

,recalculated_case_fatality_and_incident_rate AS (
    SELECT
        * EXCLUDE(incident_rate, case_fatality_ratio)
        , COALESCE(confirmed / population * 100000, 0) AS incident_rate
        , deaths / confirmed AS case_fatality_ratio
    FROM new_covid_cases
)

SELECT *
FROM recalculated_case_fatality_and_incident_rate