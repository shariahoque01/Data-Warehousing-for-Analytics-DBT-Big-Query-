{{ config(materialized="table") }}
WITH
  vehicles AS (
  SELECT
    DISTINCT coalesce(contributing_factor_vehicle_1,'None') as contributing_factor_vehicle_1,
            coalesce(contributing_factor_vehicle_2,'None') as contributing_factor_vehicle_2,
            coalesce(vehicle_type_code1,'None') as vehicle_type_code1,
            coalesce(vehicle_type_code2,'None') as vehicle_type_code2,
    -- COALESCE(contributing_factor_vehicle_1,' ') || ' ' || COALESCE(contributing_factor_vehicle_2, ' ') AS final_contributing_factor_vehicle,
    trim (COALESCE(vehicle_type_code1,' ') || ' ' || COALESCE(vehicle_type_code2, ' ') || ' ' || COALESCE(vehicle_type_code2, ' ') || ' ' || COALESCE(vehicle_type_code_3, ' ') || ' ' || COALESCE(vehicle_type_code_4, ' ')) AS final_vehicle_type
  FROM
    cis4400project-403800.projectDatasets.vehicle_collisions )
SELECT
  ROW_NUMBER() OVER () AS vehicle_collision_ID_SK,
  *
FROM
  vehicles
order by vehicle_collision_ID_SK