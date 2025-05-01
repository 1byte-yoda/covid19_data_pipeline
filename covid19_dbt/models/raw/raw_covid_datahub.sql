SELECT
    t.id
    ,t.date

    -- Location
    ,t.administrative_area_level
    ,t.country
    ,t.state
    ,t.city
    ,t.population

    -- Government Policy Measures
    ,t.school_closing
    ,t.workplace_closing
    ,t.cancel_events
    ,t.gatherings_restrictions
    ,t.transport_closing
    ,t.stay_home_restrictions
    ,t.internal_movement_restrictions
    ,t.international_movement_restrictions
    ,t.information_campaigns
    ,t.testing_policy
    ,t.contact_tracing
    ,t.facial_coverings
    ,t.vaccination_policy
    ,t.elderly_people_protection

    -- Index Policies
    ,t.stringency_index
    ,t.containment_health_index
    ,t.economic_support_index

    -- Epidemiology
    ,t.confirmed
    ,t.deaths
    ,t.recovered

    -- Tests
    ,t.tests
    ,t.vaccines
    ,t.people_vaccinated
    ,t.people_fully_vaccinated

    -- Hospitalization
    ,t.hosp
    ,t.icu
    ,t.vent
FROM {{ source('delta_source', 'covid19datahub') }} AS t
