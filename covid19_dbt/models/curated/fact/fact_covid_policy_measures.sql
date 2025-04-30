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
WHERE COALESCE(
    school_closing, workplace_closing, cancel_events, gatherings_restrictions,
    transport_closing, stay_home_restrictions, international_movement_restrictions,
    internal_movement_restrictions, information_campaigns, testing_policy,
    contact_tracing, facial_coverings, vaccination_policy, elderly_people_protection
    ) IS NOT NULL
    ),

    forward_filled AS (
SELECT
    location_id,
    date_id,

    LAST_VALUE(school_closing IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS school_closing,
    LAST_VALUE(workplace_closing IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS workplace_closing,
    LAST_VALUE(cancel_events IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cancel_events,
    LAST_VALUE(gatherings_restrictions IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gatherings_restrictions,
    LAST_VALUE(transport_closing IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS transport_closing,
    LAST_VALUE(stay_home_restrictions IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stay_home_restrictions,
    LAST_VALUE(internal_movement_restrictions IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS internal_movement_restrictions,
    LAST_VALUE(international_movement_restrictions IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS international_movement_restrictions,
    LAST_VALUE(information_campaigns IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS information_campaigns,
    LAST_VALUE(testing_policy IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS testing_policy,
    LAST_VALUE(contact_tracing IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS contact_tracing,
    LAST_VALUE(facial_coverings IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS facial_coverings,
    LAST_VALUE(vaccination_policy IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS vaccination_policy,
    LAST_VALUE(elderly_people_protection IGNORE NULLS) OVER (PARTITION BY location_id ORDER BY date_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS elderly_people_protection

FROM covid_tests
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id,
    *
FROM forward_filled