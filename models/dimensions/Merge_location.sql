{{ config(materialized="table") }}

-- next step: round latitude and longitude and on with those, EDA
--- why professor only choose the the complsint locations only
WITH
  collisions_location AS (SELECT * FROM {{ ref("collisions_location") }}),
  raw_311_location AS (SELECT * FROM {{ ref("311_location") }}),
  all_location AS (
    SELECT
    --   COALESCE(raw_311_location.street_address, collisions_location.street_address) AS street_address,
    --   COALESCE(raw_311_location.borough, collisions_location.borough) AS borough,
    --   COALESCE(raw_311_location.incident_zip, collisions_location.zip_code) AS zip_code,
    --   COALESCsE(raw_311_location.latitude, collisions_location.latitude) AS latitude,
    --   COALESCE(raw_311_location.longitude, collisions_location.longitude) AS longitude,
      collisions_location.latitude AS collision_latitude,
      collisions_location.longitude AS collision_longitude,
      TRIM(collisions_location.street_address) AS collisions_street_address,
      collisions_location.borough AS collisions_borough,
      collisions_location.zip_code AS collisions_zip_code,
      raw_311_location.latitude AS complaint_latitude,
      raw_311_location.longitude AS complaint_longitude,
      TRIM(raw_311_location.street_address) AS complaint_street_address,
      raw_311_location.borough AS complaint_borough,
      raw_311_location.incident_zip AS complaint_zip_code
    FROM
      collisions_location
    FULL JOIN
      raw_311_location
      ON (
        raw_311_location.incident_zip = collisions_location.zip_code
        OR (
          raw_311_location.incident_zip IS NULL
          AND collisions_location.zip_code IS NULL
        )
      )
      AND COALESCE(raw_311_location.borough, '') = COALESCE(collisions_location.borough, '')
    where incident_zip is not null and zip_code is not null
  )

SELECT
  ROW_NUMBER() OVER () AS locations_sks,
--   COUNT(CASE WHEN complaint_zip_code IS 
--    NULL THEN 1 END) 

*
FROM
  all_location
-- where complaint_zip_code is null
ORDER BY
  locations_sks
