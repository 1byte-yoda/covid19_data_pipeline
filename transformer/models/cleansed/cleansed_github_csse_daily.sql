{{ config(unique_key='id', incremental_strategy="delete+insert") }}
-- -----------------------------------------------------------------------------
-- Description:
--     This dbt model standardizes and cleanses COVID-19 case data sourced from
--     the GitHub CSSE feed by:
--     1. Resolving inconsistent field naming between `incidence_rate` and `incident_rate`.
--     2. Cleaning numeric values by converting them to absolute values.
--     3. Parsing timestamps using a custom macro to ensure consistent datetime formats.
--     4. Supporting incremental loading using a `delete+insert` strategy,
--        filtered by `last_update` and bounded by `min_date` and `max_date` variables.
-- -----------------------------------------------------------------------------

WITH standardized_incident_rate AS (
    SELECT
        * EXCLUDE (incidence_rate,incident_rate)
        ,COALESCE(incidence_rate,incident_rate) AS incident_rate
    FROM {{ ref('raw_github_csse_daily') }}
)

,cleansed_covid_19 AS (
    SELECT
        id

        -- Get the absolute values of numeric fields.
        ,ABS(confirmed) AS confirmed
        ,ABS(deaths) AS deaths
        ,ABS(recovered) AS recovered
        ,ABS(active) AS active
        ,ABS(case_fatality_ratio) AS case_fatality_ratio
        ,ABS(incident_rate) AS incident_rate

        -- Try Parsing timestamp field
        ,{{ try_parse_timestamp('last_update') }} AS last_update
        ,load_date
        ,year
        ,month
        ,day
    FROM standardized_incident_rate

)

SELECT
    *
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM cleansed_covid_19
WHERE
    1 = 1
    {% if is_incremental() %}
        AND last_update >= '{{ var('min_date') }}' AND last_update <= '{{ var('max_date') }}'
    {% endif %}
