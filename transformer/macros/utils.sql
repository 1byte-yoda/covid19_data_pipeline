{% macro title_case(column_name) %}
TRIM(CONCAT(
    UPPER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 1), 1, 1)),
    LOWER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 1), 2)),
    ' ',
    UPPER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 2), 1, 1)),
    LOWER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 2), 2)),
    ' ',
    UPPER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 3), 1, 1)),
    LOWER(SUBSTR(SPLIT_PART({{ column_name }}, ' ', 3), 2))
))
{% endmacro %}


