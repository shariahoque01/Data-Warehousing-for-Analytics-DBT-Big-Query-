{{ config(materialized="table") }}
SELECT
    distinct date_dim.*
   
  FROM
    cis4400project-403800.projectDatasets.311_service_requests 
left join  {{ ref('date_dim') }} as date_dim on DATE(created_date)  = full_date

order by date_dim_id