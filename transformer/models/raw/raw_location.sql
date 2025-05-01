SELECT
    id
    ,combined_key
    ,province_state
    ,country_region
    ,latitude
    ,longitude
    ,fips
    ,admin2
    ,lat
    ,longx
    ,load_date
    ,last_update
FROM {{ source('delta_source', 'github_csse_daily') }}
