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
FROM {{ source('jhu_covid', 'github_csse_daily') }}
