{% macro standardize_country(column_name) -%}
COALESCE(
    TRIM(
        CASE
            WHEN LOWER({{ column_name }}) IN ('viet nam') THEN 'Vietnam'
            WHEN LOWER({{ column_name }}) IN ('taiwan*', 'taipei and environs') THEN 'Taiwan'
            WHEN LOWER({{ column_name }}) IN ('guadeloupe', 'st. martin') THEN 'France'
            WHEN LOWER({{ column_name }}) IN ('the gambia', 'gambia, the') THEN 'Gambia'
            WHEN LOWER({{ column_name }}) IN ('the bahamas', 'bahamas, the') THEN 'Bahamas'
            WHEN LOWER({{ column_name }}) = 'cote d''ivoire' THEN 'Ivory Coast'
            WHEN LOWER({{ column_name }}) = 'north ireland' THEN 'Ireland'
            WHEN LOWER({{ column_name }}) IN ('channel islands', 'diamond princess') THEN 'United Kingdom'
            WHEN LOWER({{ column_name }}) = 'republic of ireland' THEN 'Ireland'
            WHEN LOWER({{ column_name }}) = 'republic of moldova' THEN 'Moldova'
            WHEN LOWER({{ column_name }}) = 'korea, south' THEN 'South Korea'
            WHEN LOWER({{ column_name }}) = 'czechia' THEN 'Czech Republic'
            WHEN LOWER({{ column_name }}) = 'iran (islamic republic of)' THEN 'Iran'
            WHEN LOWER({{ column_name }}) = 'congo (brazzaville)' THEN 'Republic of the Congo'
            WHEN LOWER({{ column_name }}) IN ('congo, the democratic', 'congo','congo (kinshasa)', 'congo, the democratic republic of the') THEN 'Democratic Republic of the Congo'
            WHEN LOWER({{ column_name }}) = 'burma' THEN 'Myanmar'
            WHEN LOWER({{ column_name }}) = 'republic of korea' THEN 'South Korea'
            WHEN LOWER({{ column_name }}) IN ('macao', 'macao sar') THEN 'Macau'
            WHEN LOWER({{ column_name }}) = 'hong kong sar' THEN 'Hong Kong'
            WHEN LOWER({{ column_name }}) = ' azerbaijan' THEN 'Azerbaijan'
            WHEN LOWER({{ column_name }}) = 'cape verde' THEN 'Cabo Verde'
            WHEN LOWER({{ column_name }}) IN ('occupied palestinian territory', 'west bank and gaza') THEN 'Palestine'
            WHEN LOWER({{ column_name }}) = 'korea, north' THEN 'North Korea'
            WHEN LOWER({{ column_name }}) IN ('u.s.', 'grand princess', 'us') THEN 'United States'  -- The grand princess ship was quarantined off the coast of California, and passengers were eventually disembarked in the U.S
            WHEN LOWER({{ column_name }}) IN ('china', 'mainland china', 'winter olympics 2022') THEN 'China' -- identified via lat/lng in googlemaps
            WHEN LOWER({{ column_name }}) IN ('others', 'cruise ship', 'costa atlantica', 'summer olympics 2020') THEN 'Japan'  -- identified via lat/lng in googlemaps
            WHEN LOWER({{ column_name }}) = 'ms zaandam' THEN 'Chile'  -- https://en.wikipedia.org/wiki/COVID-19_pandemic_on_cruise_ships#Zaandam
            ELSE {{ column_name }}
        END
    )
    , 'Unassigned'
)
{%- endmacro %}
