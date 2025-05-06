{{ config(unique_key='id', incremental_strategy="delete+insert") }}
WITH covid19 AS (
    SELECT
        id
        ,CASE WHEN confirmed < 0 THEN confirmed * -1 ELSE confirmed END AS confirmed
        ,CASE WHEN deaths < 0 THEN deaths * -1 ELSE deaths END AS deaths
        ,CASE WHEN recovered < 0 THEN recovered * -1 ELSE recovered END AS recovered
        ,CASE WHEN active < 0 THEN active * -1 ELSE active END AS active
        ,CASE WHEN case_fatality_ratio < 0 THEN case_fatality_ratio * -1 ELSE case_fatality_ratio END
            AS case_fatality_ratio
        ,CASE
            WHEN COALESCE(incidence_rate,incident_rate) < 0 THEN COALESCE(incidence_rate,incident_rate) * -1
            ELSE COALESCE(incidence_rate,incident_rate)
        END AS incident_rate
        ,{{ try_parse_timestamp('last_update') }} AS last_update
        ,load_date
        ,year
        ,month
        ,day
    FROM {{ ref('raw_github_csse_daily') }}

)

SELECT
    *
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM covid19
WHERE
    1 = 1
    {% if is_incremental() %}
        AND last_update >= '{{ var('min_date') }}' AND last_update <= '{{ var('max_date') }}'
    {% endif %}
