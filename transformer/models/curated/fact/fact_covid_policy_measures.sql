{{ config(unique_key='covid_id', incremental_strategy="delete+insert") }}

WITH covid_tests AS (
    SELECT DISTINCT
        location_id
        ,strftime(date::DATE,'%Y%m%d')::INT AS date_id
        ,school_closing
        ,workplace_closing
        ,cancel_events
        ,gatherings_restrictions
        ,transport_closing
        ,stay_home_restrictions
        ,internal_movement_restrictions
        ,international_movement_restrictions
        ,information_campaigns
        ,testing_policy
        ,contact_tracing
        ,facial_coverings
        ,vaccination_policy
        ,elderly_people_protection
    FROM {{ ref('cleansed_covid_datahub') }}
    WHERE
        1 = 1
        {% if is_incremental() %}
            AND date >= '{{ var('min_date') }}' AND date <= '{{ var('max_date') }}'
        {% endif %}
)

,forward_filled AS (
    SELECT DISTINCT
        location_id
        ,date_id
        ,last_value(school_closing) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS school_closing
        ,last_value(workplace_closing) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS workplace_closing
        ,last_value(cancel_events) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS cancel_events
        ,last_value(gatherings_restrictions) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS gatherings_restrictions
        ,last_value(transport_closing) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS transport_closing
        ,last_value(stay_home_restrictions) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS stay_home_restrictions
        ,last_value(internal_movement_restrictions)
            OVER (
                PARTITION BY location_id
                ORDER BY date_id
            )
            AS internal_movement_restrictions
        ,last_value(international_movement_restrictions)
            OVER (
                PARTITION BY location_id
                ORDER BY date_id
            )
            AS international_movement_restrictions
        ,last_value(information_campaigns) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS information_campaigns
        ,last_value(testing_policy) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS testing_policy
        ,last_value(contact_tracing) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS contact_tracing
        ,last_value(facial_coverings) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS facial_coverings
        ,last_value(vaccination_policy) OVER (
            PARTITION BY location_id
            ORDER BY date_id
        ) AS vaccination_policy
        ,last_value(elderly_people_protection)
            OVER (
                PARTITION BY location_id
                ORDER BY date_id
            )
            AS elderly_people_protection

    FROM covid_tests
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id', 'date_id']) }} AS covid_id
    ,*
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM forward_filled
