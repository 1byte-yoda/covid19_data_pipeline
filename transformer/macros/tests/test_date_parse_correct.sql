{% test date_parse_correct(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < '2019-01-01'

/*  -- Successful Scenario
GIVEN: 3/8/20 5:19
WHEN: try_parse_timestamp('3/8/20 5:19')
THEN: 2020-03-08 05:19:00
*/

/*  -- Failed Scenario
GIVEN: 3/8/20 5:19
WHEN: try_parse_timestamp('3/8/20 5:19')
THEN: 0020-03-08 05:19:00
*/

{% endtest %}