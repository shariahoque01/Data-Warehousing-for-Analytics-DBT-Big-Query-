{{ config(materialized="table") }}
-- list of 311 dimentions:
-- 311 dimentions:
-- Location
-- Complaint type
-- status
-- agency
-- date

-- -- non-null location 
with
    all_complaints_data as (select * from {{ ref("All_complaint_data") }}),
    complaint_location as (select * from {{ ref("311_location") }}),
    complaint_type as (select * from {{ ref("complaint_type") }}),
    status as (select * from {{ ref("Status") }}),
    agecny as (select * from {{ ref("agency") }}),
    dates as (select * from {{ ref("date_dim") }}),

all_ids as (select
    complaint_type_descriptor_sks,
    location_id_sks,
    agecny.agency_id_sk,
    status_sks,
    date_dim_id
from all_complaints_data
left join
    complaint_location
    on all_complaints_data.latitude = complaint_location.latitude
    or (all_complaints_data.latitude is null and complaint_location.latitude is null)
    and all_complaints_data.longitude = complaint_location.longitude
    or (all_complaints_data.longitude is null and complaint_location.longitude is null)
    and all_complaints_data.borough = complaint_location.borough
    or (all_complaints_data.borough is null and complaint_location.borough is null)
    and all_complaints_data.city = complaint_location.city
    or (all_complaints_data.city is null and complaint_location.city is null)
    and all_complaints_data.incident_zip = complaint_location.incident_zip
    or (
        all_complaints_data.incident_zip is null
        and complaint_location.incident_zip is null
    )
    and all_complaints_data.street_address = complaint_location.street_address
    or (
        all_complaints_data.street_address is null
        and complaint_location.street_address is null
    )
left join
    complaint_type
    on all_complaints_data.complaint_type = complaint_type.complaint_type
    and all_complaints_data.descriptor = complaint_type.descriptor
left join status on all_complaints_data.status = status.status
left join dates on all_complaints_data.created_date = dates.full_date
left join agecny on all_complaints_data.agency = agecny.agency)

select row_number()over() as Main_Ids_Sks, *
from all_ids
order by Main_Ids_Sks

