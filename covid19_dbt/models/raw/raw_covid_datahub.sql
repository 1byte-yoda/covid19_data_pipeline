SELECT t.id,
    date,

    -- Location
    t.administrative_area_level,
    t.country,
    t.state,
    t.city,
    t.population,

    -- Government Policy Measures
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
    elderly_people_protection,

    -- Index Policies
    t.stringency_index,
    t.containment_health_index,
    economic_support_index,

    -- Epidemiology
    t.confirmed,
    t.deaths,
    t.recovered,

    -- Tests
    t.tests,
    t.vaccines,
    t.people_vaccinated,
    t.people_fully_vaccinated,

    -- Hospitalization
    t.hosp,
    t.icu,
    t.vent
FROM {{ source('jhu_covid', 'covid19datahub') }} AS t
