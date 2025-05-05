{% macro standardize_country(column_name) -%}

TRIM(
    CASE
        WHEN LOWER({{ column_name }}) IN ('viet nam') THEN 'Vietnam'
        WHEN LOWER({{ column_name }}) IN ('taiwan*', 'taipei and environs') THEN 'Taiwan'
        WHEN LOWER({{ column_name }}) IN ('guadeloupe', 'st. martin') THEN 'France'
        WHEN LOWER({{ column_name }}) IN ('the gambia', 'gambia, the') THEN 'Gambia'
        WHEN LOWER({{ column_name }}) IN ('the bahamas', 'bahamas, the') THEN 'Bahamas'
        WHEN LOWER({{ column_name }}) = 'cote d''ivoire' THEN 'Ivory Coast'
        WHEN LOWER({{ column_name }}) = 'north ireland' THEN 'Ireland'
        WHEN LOWER({{ column_name }}) = 'diamond princess' THEN 'United Kingdom'
        WHEN LOWER({{ column_name }}) = 'republic of ireland' THEN 'Ireland'
        WHEN LOWER({{ column_name }}) = 'republic of moldova' THEN 'Moldova'
        WHEN LOWER({{ column_name }}) = 'korea, south' THEN 'South Korea'
        WHEN LOWER({{ column_name }}) = 'czechia' THEN 'Czech Republic'
        WHEN LOWER({{ column_name }}) = 'iran (islamic republic of)' THEN 'Iran'
        WHEN LOWER({{ column_name }}) IN ('congo (kinshasa)', 'congo (brazzaville)', 'republic of the congo') THEN 'Congo'
        WHEN LOWER({{ column_name }}) = 'burma' THEN 'Myanmar'
        WHEN LOWER({{ column_name }}) = 'republic of korea' THEN 'South Korea'
        WHEN LOWER({{ column_name }}) = 'macao sar' THEN 'Macau'
        WHEN LOWER({{ column_name }}) = 'hong kong sar' THEN 'Hong Kong'
        WHEN LOWER({{ column_name }}) = ' azerbaijan' THEN 'Azerbaijan'
        WHEN LOWER({{ column_name }}) = 'cape verde' THEN 'Cabo Verde'
        WHEN LOWER({{ column_name }}) IN ('occupied palestinian territory', 'west bank and gaza') THEN 'Palestine'
        WHEN LOWER({{ column_name }}) = 'korea, north' THEN 'North Korea'
        WHEN LOWER({{ column_name }}) = 'us' THEN 'United States'
        WHEN LOWER({{ column_name }}) IN ('china', 'winter olympics 2022') THEN 'Mainland China' -- identified via lat/lng in googlemaps
        WHEN LOWER({{ column_name }}) IN ('others', 'cruise ship', 'summer olympics 2020') THEN 'Japan'  -- identified via lat/lng in googlemaps
        WHEN LOWER({{ column_name }}) = 'ms zaandam' THEN 'Chile'  -- https://en.wikipedia.org/wiki/COVID-19_pandemic_on_cruise_ships#Zaandam
        ELSE {{ column_name }}
    END
)

{%- endmacro %}
