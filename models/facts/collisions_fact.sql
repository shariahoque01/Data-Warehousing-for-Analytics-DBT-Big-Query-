{{ config(materialized="table") }}

-- list of collisions dimentions:
-- all data
-- Location
-- time
-- vehicles
-- date
-- -- non-null location 
WITH
    all_collision_data AS (SELECT * FROM {{ ref("all_Collisions_data") }}),
    collisions_location AS (SELECT * FROM {{ ref("collisions_location") }}),
    types_of_vehicles AS (SELECT * FROM {{ ref("vehicles") }}),
    time_dim AS (SELECT * FROM {{ ref("time_dim") }}),
    dates AS (SELECT * FROM {{ ref("date_dim") }}),

all_ids as( SELECT
    date_dim_id,
    hour_id_sk,
    collision_id,
    collision_location_sk,
    vehicle_collision_id_sk,
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,
    total_killed,
    total_harmed_killed_injured
FROM
    all_collision_data
LEFT JOIN
    collisions_location ON
    (
        all_collision_data.latitude = collisions_location.latitude OR
        (all_collision_data.latitude IS NULL AND collisions_location.latitude IS NULL)
    )
    AND (
        all_collision_data.longitude = collisions_location.longitude OR
        (all_collision_data.longitude IS NULL AND collisions_location.longitude IS NULL)
    )
    AND (
        all_collision_data.borough = collisions_location.borough OR
        (all_collision_data.borough IS NULL AND collisions_location.borough IS NULL)
    )
    AND (
        all_collision_data.zip_code = collisions_location.zip_code OR
        (all_collision_data.zip_code IS NULL AND collisions_location.zip_code IS NULL)
    )
    AND (
        all_collision_data.street_address = collisions_location.street_address OR
        (all_collision_data.street_address IS NULL AND collisions_location.street_address IS NULL)
    )
LEFT JOIN
    types_of_vehicles ON
    (
        all_collision_data.final_vehicle_type = types_of_vehicles.final_vehicle_type OR
        (all_collision_data.final_vehicle_type IS NULL AND types_of_vehicles.final_vehicle_type IS NULL)
    )
    AND (
        all_collision_data.contributing_factor_vehicle_1 = types_of_vehicles.contributing_factor_vehicle_1 OR
        (all_collision_data.contributing_factor_vehicle_1 IS NULL AND types_of_vehicles.contributing_factor_vehicle_1 IS NULL)
    )
    AND (
        all_collision_data.contributing_factor_vehicle_2 = types_of_vehicles.contributing_factor_vehicle_2 OR
        (all_collision_data.contributing_factor_vehicle_2 IS NULL AND types_of_vehicles.contributing_factor_vehicle_2 IS NULL)
    )
LEFT JOIN
    time_dim ON EXTRACT(HOUR FROM TIME(PARSE_TIME('%H:%M', crash_time))) = hour
and EXTRACT(MINUTE FROM TIME(PARSE_TIME('%H:%M', crash_time))) = minute
LEFT JOIN
    dates ON all_collision_data.crash_date = dates.full_date)

select row_number() over() as main_ids, * 
from all_ids
order by main_ids