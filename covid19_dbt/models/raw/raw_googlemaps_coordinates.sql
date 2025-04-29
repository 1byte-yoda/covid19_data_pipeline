SELECT combined_key,
       longitude,
       latitude
FROM {{ source('csv_source', 'location_coordinates') }}
