{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT
        DATEADD(day, seq4(), '2020-01-01')::DATE AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)

SELECT
    date_key,
    YEAR(date_key) AS year,
    QUARTER(date_key) AS quarter,
    MONTH(date_key) AS month,
    DAY(date_key) AS day,
    DAYOFWEEK(date_key) AS day_of_week,
    DAYNAME(date_key) AS day_name,
    MONTHNAME(date_key) AS month_name,
    CONCAT('Q', QUARTER(date_key)) AS quarter_name,
    CONCAT(YEAR(date_key), '-Q', QUARTER(date_key)) AS year_quarter,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN FALSE ELSE TRUE END AS is_weekday
FROM date_spine
