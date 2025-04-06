{{ config(materialized="table") }}
-- add sks
with
    all_collision_data as (
        select
            collision_id,
            cast(crash_date as date) crash_date,
            crash_time, 
            latitude,
            longitude,
            coalesce(cross_street_name, on_street_name)
            || ' '
            || coalesce(on_street_name, ' ')
            || ' '
            || coalesce(off_street_name, ' ') as street_address,
            zip_code,
            borough,
            coalesce(contributing_factor_vehicle_1,'None') as contributing_factor_vehicle_1,
            coalesce(contributing_factor_vehicle_2,'None') as contributing_factor_vehicle_2,
            coalesce(vehicle_type_code1,'None') as vehicle_type_code1,
            coalesce(vehicle_type_code2,'None') as vehicle_type_code2,
            trim(
                coalesce(vehicle_type_code1, ' ')
                || ' '
                || coalesce(vehicle_type_code2, ' ')
                || ' '
                || coalesce(vehicle_type_code2, ' ')
                || ' '
                || coalesce(vehicle_type_code_3, ' ')
                || ' '
                || coalesce(vehicle_type_code_4, ' ')
            ) as final_vehicle_type,
            ifnull(number_of_persons_injured, 0) as number_of_persons_injured,
            ifnull(number_of_persons_killed, 0) as number_of_persons_killed,
            ifnull(number_of_pedestrians_injured, 0) as number_of_pedestrians_injured,
            ifnull(number_of_pedestrians_killed, 0) as number_of_pedestrians_killed,
            ifnull(number_of_cyclist_injured, 0) as number_of_cyclist_injured,
            ifnull(number_of_cyclist_killed, 0) as number_of_cyclist_killed,
            ifnull(number_of_motorist_injured, 0) as number_of_motorist_injured,
            ifnull(number_of_motorist_killed, 0) as number_of_motorist_killed

        from cis4400project-403800.projectDatasets.vehicle_collisions
    )

,all_data_accident as (select
    *,
    number_of_persons_killed
    + number_of_motorist_killed
    + number_of_cyclist_killed
    + number_of_pedestrians_killed as total_killed,
    number_of_persons_killed
    + number_of_motorist_killed
    + number_of_cyclist_killed
    + number_of_pedestrians_killed
    + number_of_persons_injured
    + number_of_pedestrians_injured
    + number_of_cyclist_injured
    + number_of_motorist_injured as total_harmed_killed_injured

from all_collision_data
where zip_code is not null )

select row_number() over() as all_collisions_ids, * from all_data_accident

order by all_collisions_ids