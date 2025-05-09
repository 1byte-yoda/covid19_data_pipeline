WITH generate_date AS (
    SELECT CAST(range AS DATE) AS date_value
    FROM
        RANGE(DATE '2020-01-01',DATE '2030-12-31',INTERVAL 1 DAY)
)

SELECT
    date_value
    ,CAST(STRFTIME(date_value,'%Y%m%d') AS INT) AS date_id
    ,DAYOFYEAR(date_value) AS day_of_year
    ,DAYNAME(date_value) AS day_name
    ,YEARWEEK(date_value) AS week_key
    ,WEEKOFYEAR(date_value) AS week_of_year
    ,MONTH(date_value) AS month_of_year
    ,YEAR(date_value) || RIGHT('0' || MONTH(date_value),2) AS month_key
    ,DAYOFMONTH(date_value) AS day_of_month
    ,LEFT(MONTHNAME(date_value),3) AS month_name_short
    ,MONTHNAME(date_value) AS month_name
    ,CAST(YEAR(date_value) || QUARTER(date_value) AS INT) AS quarter_key
    ,QUARTER(date_value) AS quarter_of_year
    ,CAST(YEAR(date_value) AS INT) AS year_key
    ,NOW() AT TIME ZONE 'UTC' AS inserted_at
FROM generate_date
