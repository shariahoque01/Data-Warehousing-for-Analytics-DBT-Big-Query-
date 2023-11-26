{{ config(materialized="table") }}


with
    complaint_type as (

        select distinct (complaint_type), descriptor

        from `cis4400project-403800.projectDatasets.311_service_requests`

    )

select row_number() over (order by (descriptor)) as sks, *
from
    complaint_type

    -- select
    -- distinct(Complaint_Type), Descriptor, row_number() over (order by
    -- distinct(Descriptor)) as SKs
    -- FROM `cis4400project-403800.projectDatasets.311_traffic_lights`
    
