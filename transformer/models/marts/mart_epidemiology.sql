{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model produces a comprehensive fact table that consolidates:
--     - COVID-19 case metrics (confirmed, deaths, recovered, etc.)
--     - Hospitalization data (hospitalized, ICU, ventilated)
--     - Policy measures (school closures, mask mandates, etc.)
--     - Testing and vaccination daily deltas
--     - Location metadata (country, state, city, ISO codes, population, etc.)
--     - Date metadata (calendar attributes like month, quarter, year)
--
--     This table serves as the primary analytical layer for our BI tool,
--     joining all the covid fact tables and dimensions.
-- -----------------------------------------------------------------------------

SELECT DISTINCT
    f1.covid_id
    ,dd.date_id AS date_key
    ,dd.date_value
    ,dd.month_key
    ,dd.month_name_short AS month
    ,dd.quarter_key
    ,dd.year_key
    ,dl.country
    ,dl.state
    ,dl.city
    ,dl.longitude
    ,dl.latitude
    ,dl.administrative_area_level
    ,dl.iso2
    ,dl.iso3
    ,dl.continent
    ,dl.population
    ,f1.confirmed
    ,f1.deaths
    ,f1.recovered
    ,f1.active
    ,f1.incident_rate
    ,f1.case_fatality_ratio
    ,f2.hosp
    ,f2.icu
    ,f2.vent
    ,f3.school_closing
    ,f3.workplace_closing
    ,f3.cancel_events
    ,f3.gatherings_restrictions
    ,f3.transport_closing
    ,f3.stay_home_restrictions
    ,f3.international_movement_restrictions
    ,f3.internal_movement_restrictions
    ,f3.information_campaigns
    ,f3.testing_policy
    ,f3.contact_tracing
    ,f3.facial_coverings
    ,f3.vaccination_policy
    ,f3.elderly_people_protection
    ,f4.tests
    ,f4.vaccines
    ,f4.people_vaccinated
    ,f4.people_fully_vaccinated
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM {{ ref('fact_covid_cases') }} AS f1
INNER JOIN {{ ref('dim_location') }} AS dl ON f1.location_id = dl.location_id
INNER JOIN {{ ref('dim_date') }} AS dd ON f1.date_id = dd.date_id
LEFT JOIN {{ ref('fact_covid_hospitalization') }} AS f2 ON f1.covid_id = f2.covid_id
LEFT JOIN {{ ref('fact_covid_policy_measures') }} AS f3 ON f1.covid_id = f3.covid_id
LEFT JOIN {{ ref('fact_covid_tests') }} AS f4 ON f1.covid_id = f4.covid_id
WHERE
    1 = 1
    {% if is_incremental() %}
        AND dd.date_value >= '{{ var('min_date') }}' AND dd.date_value <= '{{ var('max_date') }}'
    {% endif %}
