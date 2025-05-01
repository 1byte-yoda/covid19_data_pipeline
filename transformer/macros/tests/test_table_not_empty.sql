
{% test table_not_empty(model) %}
    WITH row_count AS (
        SELECT COUNT(1) AS total_rows
        FROM {{ model }}
    )

    SELECT *
    FROM row_count
    WHERE total_rows = 0
{% endtest %}