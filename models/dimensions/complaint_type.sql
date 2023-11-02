{{
    config(
        materialized = 'table'
    )
}}
with complaint_type as (

    select
        distinct(Complaint_Type), Descriptor

FROM `cis4400project-403800.projectDatasets.311_traffic_lights`

)

select * from complaint_type

