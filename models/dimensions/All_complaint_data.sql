{{ config(materialized="table") }}

-- non-null location 
with
    complaint_data as (
        select
            unique_key,
            cast(created_date as date) created_date,
            cast(closed_date as date) closed_date,
            agency,
            agency_name,
            complaint_type,
            descriptor,
            status,
            incident_zip,
            ifnull(borough, city) as borough,
            ifnull(city, borough) as city,
            TRIM(COALESCE(intersection_street_1, cross_street_1) ||COALESCE(' ') || COALESCE(intersection_street_2, cross_street_2)) AS street_address,
            latitude,
            longitude
        from cis4400project-403800.projectDatasets.311_service_requests
    )

select 

ROW_NUMBER() over() as complaint_data_SKs,*
from complaint_data
where incident_zip is not null 
