{{ config(materialized="table") }}

WITH dim_time AS (
  SELECT
    t,
    EXTRACT(HOUR FROM t) AS hour,
    EXTRACT(MINUTE FROM t) AS minute
  FROM
    UNNEST(GENERATE_TIMESTAMP_ARRAY('2023-01-01T00:00:00', '2023-01-01T23:59:59', INTERVAL 1 MINUTE)) AS t
)

SELECT
  ROW_NUMBER() OVER (ORDER BY hour, minute) AS hour_id_sk,
  FORMAT_TIMESTAMP('%H:%M', t) AS real_time,
  hour,
  minute,
  CASE
    WHEN hour >= 6 AND hour < 12 THEN 'Morning'
    WHEN hour >= 12 AND hour < 18 THEN 'Afternoon'
    ELSE 'Night'
  END AS time_of_day
FROM
  dim_time
order by hour_id_sk
