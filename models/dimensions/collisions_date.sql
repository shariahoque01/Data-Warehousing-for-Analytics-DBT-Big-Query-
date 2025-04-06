{{ config(materialized="table") }}
SELECT
    distinct date_dim.*
   
  FROM
    cis4400project-403800.projectDatasets.vehicle_collisions 
left join  {{ ref('date_dim') }} as date_dim on DATE(crash_date)  = full_date

order by date_dim_id