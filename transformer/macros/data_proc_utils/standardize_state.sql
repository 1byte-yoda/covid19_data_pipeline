{% macro standardize_state(column_name) -%}
COALESCE(
    TRIM(
        CASE
            WHEN LOWER({{ column_name }}) IN ('us', 'netherlands') THEN 'Unassigned'
            WHEN LOWER({{ column_name }}) LIKE '%w.p%' THEN REPLACE({{ column_name }}, 'W.P.', '')
            WHEN LOWER({{ column_name }}) LIKE 'st.%' THEN REPLACE({{ column_name }}, 'St.', 'Saint')
            WHEN LOWER({{ column_name }}) LIKE 'st%' THEN REPLACE({{ column_name }}, 'St', 'Saint')
            WHEN LOWER({{ column_name }}) LIKE '%diamond princess%' THEN 'Others'
            ELSE {{ column_name }}
        END
    )
    ,'Unassigned'
)

{%- endmacro %}