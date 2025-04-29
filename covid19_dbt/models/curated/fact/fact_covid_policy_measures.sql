WITH covid_tests AS (
    SELECT DISTINCT
        location_id
       ,{{ dbt_utils.generate_surrogate_key(['date']) }} AS date_id
       , school_closing
       , workplace_closing
       , cancel_events
       , gatherings_restrictions
       , transport_closing
       , stay_home_restrictions
       , internal_movement_restrictions
       , international_movement_restrictions
       , information_campaigns
       , testing_policy
       , contact_tracing
       , facial_coverings
       , vaccination_policy
       , elderly_people_protection
    FROM {{ ref('cleansed_covid_datahub') }} AS t
)

SELECT {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id, *
FROM covid_tests