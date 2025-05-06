{{ config(unique_key='row_key', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model processes and standardizes raw COVID-19 data by:
--     1. Standardizing location fields (country, state, city) using macros.
--     2. Cleansing and transforming key fields: filling nulls, formatting strings,
--        and ensuring numeric values are positive.
--     3. Generating derived fields, including a combined location key and
--        a surrogate `location_id` for uniqueness.
--     4. Supporting incremental loading using a `delete+insert` strategy
--        bounded by `min_date` and `max_date` variables.
-- -----------------------------------------------------------------------------

WITH standardized_location_fields AS (
    SELECT
        * EXCLUDE (country,state,city)
        ,{{ standardize_country('country') }} AS country
        ,{{ standardize_state('state') }} AS state
        ,COALESCE(city,'Unassigned') AS city
    FROM {{ ref ('raw_covid_datahub') }}
)

,cleansed_covid_data_hub AS (
    SELECT
        row_key
        ,id
        ,administrative_area_level
        ,{{ title_case('country' ) }} AS country
        ,{{ title_case('state' ) }} AS state
        ,{{ title_case('city' ) }} AS city

        -- Lowercased location fields to be used to standardize location surrogate key
        ,LOWER(state) AS low_state
        ,LOWER(city) AS low_city
        ,LOWER(country) AS low_country

        -- Fill Null Discrete Values with 0
        ,COALESCE(population,0) AS population
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
        ,COALESCE(date,'1900-01-01')::DATE AS date

        -- Convert Numeric Values to Positive value
        ,ABS(stringency_index) AS stringency_index
        ,ABS(containment_health_index) AS containment_health_index
        ,ABS(economic_support_index) AS economic_support_index
        ,ABS(confirmed) AS confirmed
        ,ABS(deaths) AS deaths
        ,ABS(recovered) AS recovered
        ,ABS(tests) AS tests
        ,ABS(vaccines) AS vaccines
        ,ABS(people_vaccinated) AS people_vaccinated
        ,ABS(people_fully_vaccinated) AS people_fully_vaccinated
        ,ABS(hosp) AS hosp
        ,ABS(icu) AS icu
        ,ABS(vent) AS vent
    FROM standardized_location_fields
)

,derived_covid_fields AS (
    SELECT
        * EXCLUDE (low_country,low_state,low_city,country,state,city)
        ,country
        ,state
        ,city

        -- Derive combined_key from the cleansed location fields
        ,CASE
            WHEN administrative_area_level = 3 THEN CONCAT(city,', ',state,', ',country)
            WHEN administrative_area_level = 2 THEN CONCAT(state,', ',country)
            ELSE country
        END AS combined_key

        -- Generate location_id from lower cased location fields
        ,{{ dbt_utils.generate_surrogate_key(['low_country', 'low_state', 'low_city', 'administrative_area_level']) }} AS location_id
    FROM cleansed_covid_data_hub
)

SELECT
    *
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM derived_covid_fields
WHERE
    1 = 1
    {% if is_incremental() %}
        AND date >= '{{ var('min_date') }}' AND date <= '{{ var('max_date') }}'
    {% endif %}
