{{ config(materialized="table") }}


with
    complaint_type as (

        select distinct (complaint_type), descriptor

        from `cis4400project-403800.projectDatasets.311_service_requests`

    )

select row_number() over (order by (descriptor)) as complaint_type_descriptor_SKs, *
from
    complaint_type
    order by complaint_type_descriptor_SKs


