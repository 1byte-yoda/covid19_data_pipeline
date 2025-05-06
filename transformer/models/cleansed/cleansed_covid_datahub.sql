{{ config(unique_key='row_key', incremental_strategy="delete+insert") }}
WITH covid_datahub AS (
    SELECT
        t.row_key
        ,t.id
        ,COALESCE(t.date,'1900-01-01')::DATE AS date
        ,t.administrative_area_level
        ,COALESCE({{ standardize_country('country') }},'Unassigned') AS country
        ,COALESCE({{ standardize_state('state') }},'Unassigned') AS state
        ,COALESCE(t.city,'Unassigned') AS city
        ,LOWER(COALESCE({{ standardize_state('state') }},'Unassigned')) AS low_state
        ,LOWER(COALESCE(t.city,'Unassigned')) AS low_city
        ,LOWER(COALESCE({{ standardize_country('country') }},'Unassigned')) AS low_country
        ,t.population
        ,COALESCE(school_closing,0) AS school_closing
        ,COALESCE(workplace_closing,0) AS workplace_closing
        ,COALESCE(cancel_events,0) AS cancel_events
        ,COALESCE(gatherings_restrictions,0) AS gatherings_restrictions
        ,COALESCE(transport_closing,0) AS transport_closing
        ,COALESCE(stay_home_restrictions,0) AS stay_home_restrictions
        ,COALESCE(internal_movement_restrictions,0) AS internal_movement_restrictions
        ,COALESCE(international_movement_restrictions,0) AS international_movement_restrictions
        ,COALESCE(information_campaigns,0) AS information_campaigns
        ,COALESCE(testing_policy,0) AS testing_policy
        ,COALESCE(contact_tracing,0) AS contact_tracing
        ,COALESCE(facial_coverings,0) AS facial_coverings
        ,COALESCE(vaccination_policy,0) AS vaccination_policy
        ,COALESCE(elderly_people_protection,0) AS elderly_people_protection
        ,

        -- Index Policies
        CASE WHEN t.stringency_index < 0 THEN t.stringency_index * -1 ELSE t.stringency_index END AS stringency_index
        ,CASE WHEN t.containment_health_index < 0 THEN t.containment_health_index * -1 ELSE t.containment_health_index END AS containment_health_index
        ,CASE WHEN t.economic_support_index < 0 THEN t.economic_support_index * -1 ELSE t.economic_support_index END AS economic_support_index
        ,

        -- Epidemiology
        t.confirmed
        ,t.deaths
        ,CASE WHEN recovered < 0 THEN recovered * -1 ELSE recovered END AS recovered
        ,

        -- Tests
        t.tests
        ,t.vaccines
        ,t.people_vaccinated
        ,t.people_fully_vaccinated
        ,

        -- Hospitalization
        t.hosp
        ,t.icu
        ,t.vent
    FROM {{ ref ('raw_covid_datahub') }} AS t
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['low_country', 'low_state', 'low_city', 'administrative_area_level']) }} AS location_id
    ,{{ title_case('country' ) }} AS country
    ,{{ title_case('state' ) }} AS state
    ,{{ title_case('city' ) }} AS city
    ,CASE
        WHEN administrative_area_level = 3 THEN CONCAT(city,', ',state,', ',country)
        WHEN administrative_area_level = 2 THEN CONCAT(state,', ',country)
        ELSE country
    END AS combined_key
    ,* EXCLUDE (low_country,low_state,low_city,country,state,city)
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM covid_datahub
WHERE
    1 = 1
    {% if is_incremental() %}
        AND date >= '{{ var('min_date') }}' AND date <= '{{ var('max_date') }}'
    {% endif %}
