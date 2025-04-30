WITH covid_tests AS (
    SELECT DISTINCT
        location_id,
        strftime(date, '%Y%m%d')::INT AS date_id,
            school_closing,
        workplace_closing,
        cancel_events,
        gatherings_restrictions,
        transport_closing,
        stay_home_restrictions,
        internal_movement_restrictions,
        international_movement_restrictions,
        information_campaigns,
        testing_policy,
        contact_tracing,
        facial_coverings,
        vaccination_policy,
        elderly_people_protection
    FROM {{ ref('cleansed_covid_datahub') }} AS t
-- WHERE COALESCE(
--     school_closing, workplace_closing, cancel_events, gatherings_restrictions,
--     transport_closing, stay_home_restrictions, international_movement_restrictions,
--     internal_movement_restrictions, information_campaigns, testing_policy,
--     contact_tracing, facial_coverings, vaccination_policy, elderly_people_protection
--     ) IS NOT NULL
    ),

    forward_filled AS (
SELECT DISTINCT
    location_id,
    date_id,
    LAST_VALUE(school_closing) OVER (PARTITION BY location_id ORDER BY date_id) AS school_closing,
    LAST_VALUE(workplace_closing) OVER (PARTITION BY location_id ORDER BY date_id) AS workplace_closing,
    LAST_VALUE(cancel_events) OVER (PARTITION BY location_id ORDER BY date_id) AS cancel_events,
    LAST_VALUE(gatherings_restrictions) OVER (PARTITION BY location_id ORDER BY date_id) AS gatherings_restrictions,
    LAST_VALUE(transport_closing) OVER (PARTITION BY location_id ORDER BY date_id) AS transport_closing,
    LAST_VALUE(stay_home_restrictions) OVER (PARTITION BY location_id ORDER BY date_id) AS stay_home_restrictions,
    LAST_VALUE(internal_movement_restrictions) OVER (PARTITION BY location_id ORDER BY date_id) AS internal_movement_restrictions,
    LAST_VALUE(international_movement_restrictions) OVER (PARTITION BY location_id ORDER BY date_id) AS international_movement_restrictions,
    LAST_VALUE(information_campaigns) OVER (PARTITION BY location_id ORDER BY date_id) AS information_campaigns,
    LAST_VALUE(testing_policy) OVER (PARTITION BY location_id ORDER BY date_id) AS testing_policy,
    LAST_VALUE(contact_tracing) OVER (PARTITION BY location_id ORDER BY date_id) AS contact_tracing,
    LAST_VALUE(facial_coverings) OVER (PARTITION BY location_id ORDER BY date_id) AS facial_coverings,
    LAST_VALUE(vaccination_policy) OVER (PARTITION BY location_id ORDER BY date_id) AS vaccination_policy,
    LAST_VALUE(elderly_people_protection) OVER (PARTITION BY location_id ORDER BY date_id) AS elderly_people_protection

FROM covid_tests
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id,
    *
FROM forward_filled