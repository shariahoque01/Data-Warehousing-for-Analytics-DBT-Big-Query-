{{ config(materialized="table") }}

WITH
  location AS (
  SELECT
    DISTINCT latitude,
    longitude,
    -- on_street_name,
    -- off_street_name,
    -- ifnull(cross_street_name, on_street_name) as cross_street_name,
    COALESCE(cross_street_name, on_street_name) ||' ' || COALESCE(on_street_name, ' ') || ' '||COALESCE(off_street_name, ' ') AS street_address,
    zip_code,
    borough
  FROM
    cis4400project-403800.projectDatasets.vehicle_collisions )
SELECT
  ROW_NUMBER() OVER (ORDER BY latitude, longitude, street_address, zip_code, borough ) AS collision_location_SK,
  *
FROM
  location

order by collision_location_SK