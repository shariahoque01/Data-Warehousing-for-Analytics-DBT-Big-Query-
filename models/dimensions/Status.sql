{{ config(materialized="table") }}

WITH
  dim_status AS (
  SELECT
    status,
    DENSE_RANK() OVER (ORDER BY status) AS Status_SKs
  FROM
    cis4400project-403800.projectDatasets.311_service_requests )
SELECT
  DISTINCT Status_SKs,
  status
FROM
  dim_status
  order by Status_SKs