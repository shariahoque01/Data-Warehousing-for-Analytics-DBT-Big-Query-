{{ config(materialized="table") }}

WITH
    collisions_date AS (SELECT * FROM {{ ref('collisions_date') }}),
    complaint_date AS (SELECT * FROM {{ ref('complaint_date') }})

SELECT DISTINCT
    collisions_date.date_dim_id,
    collisions_date.full_date,
    collisions_date.year,
    -- collisions_date.year_week,
    -- collisions_date.year_day,
    -- collisions_date.fiscal_year,
    -- collisions_date.fiscal_qtr,
    collisions_date.month,
    collisions_date.month_name,
    collisions_date.week_day,
    collisions_date.day_name,
    -- collisions_date.day_is_weekday
FROM
    collisions_date
FULL JOIN
    complaint_date ON collisions_date.full_date = complaint_date.full_date
where collisions_date.date_dim_id is not null
order by date_dim_id