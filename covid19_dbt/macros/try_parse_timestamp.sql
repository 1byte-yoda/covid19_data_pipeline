{% macro try_parse_timestamp(column_name) -%}
COALESCE(
  TRY_STRPTIME({{ column_name }}, '%Y-%m-%dT%H:%M:%S'),      -- 2023-01-06T04:21:02
  TRY_STRPTIME({{ column_name }}, '%Y-%m-%d %H:%M:%S'),      -- 2023-01-06 04:21:02
  TRY_STRPTIME({{ column_name }}, '%Y-%m-%d %H:%M'),         -- 2021-01-15 17:22
  TRY_STRPTIME({{ column_name }}, '%-m/%-d/%y %-H:%M'),      -- 3/8/20 5:19
  TRY_STRPTIME({{ column_name }}, '%-m/%-d/%y %H:%M'),       -- 1/23/20 17:00
  TRY_STRPTIME({{ column_name }}, '%-m/%-d/%Y %H:%M'),       -- 1/22/2020 17:00
  TRY_STRPTIME({{ column_name }}, '%Y/%m/%d %H:%M:%S'),      -- 2023/01/06 04:21:02
  TRY_STRPTIME({{ column_name }}, '%m/%d/%Y %H:%M'),         -- 03/08/2020 05:19
  TRY_STRPTIME({{ column_name }}, '%m/%d/%y %H:%M:%S')       -- 03/08/20 05:19:00
)

{%- endmacro %}
