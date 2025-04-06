{{ config(materialized="table") }}
WITH
  location AS (
  SELECT
    DISTINCT 
    COALESCE(ifnull(borough, City),'None') as borough,
    COALESCE(ifnull(City, borough),'None') as city,
    COALESCE(incident_zip,00000) as incident_zip,
 --   ifnull(intersection_street_1,cross_street_1) as intersection_street_1,
  --  ifnull(intersection_street_2,cross_street_2) as intersection_street_2,
TRIM(COALESCE(intersection_street_1, cross_street_1) ||COALESCE(' ') || COALESCE(intersection_street_2, cross_street_2)) AS street_address,
    COALESCE(longitude,0.00) as longitude,
    COALESCE(latitude,0.00) as latitude
    
  FROM
    cis4400project-403800.projectDatasets.311_service_requests)
 
  SELECT
    ROW_NUMBER() OVER (ORDER BY incident_zip) AS location_id_SKs,
    *
   -- ,case when cross_street_1 = intersection_street_1 then 1 else 0 end as borough_equal_city
  FROM
    location 
  order by location_id_SKs


