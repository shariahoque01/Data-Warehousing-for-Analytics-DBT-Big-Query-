{{ config(materialized="table") }}

with agencies as (

    select distinct
    agency,
    agency_name

FROM cis4400project-403800.projectDatasets.311_service_requests

)


select 
 row_number() over (order by 
    agency,
    agency_name) as agency_ID_SK,*
from agencies