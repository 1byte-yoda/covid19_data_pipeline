SELECT
    id
    ,confirmed
    ,deaths
    ,recovered
    ,active
    ,incident_rate
    ,incidence_rate
    ,case_fatality_ratio
    ,last_update
    ,load_date
    ,year
    ,month
    ,day
FROM {{ source('delta_source', 'github_csse_daily') }}
