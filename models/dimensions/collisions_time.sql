{{ config(materialized="table") }}


select DISTINCT 

  time_dim.*,
  crash_time
  from cis4400project-403800.projectDatasets.vehicle_collisions

left join {{ ref('time_dim') }} as time_dim
on EXTRACT(HOUR FROM TIME(PARSE_TIME('%H:%M', crash_time))) = hour
and EXTRACT(MINUTE FROM TIME(PARSE_TIME('%H:%M', crash_time))) = minute

order by crash_time

