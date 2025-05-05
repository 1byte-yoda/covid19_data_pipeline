{% macro title_case(column_name) %}
CONCAT(
    UPPER(SUBSTR({{ column_name }}, 1, 1)),
    LOWER(SUBSTR({{ column_name }}, 2))
)
{% endmacro %}
