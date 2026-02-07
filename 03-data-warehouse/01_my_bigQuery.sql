-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-3.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://green_tripdata_2020/green_tripdata_2020.csv']
);


-- Check green trip data
SELECT *
FROM `zoomcamp-3.ny_taxi.external_green_tripdata`
LIMIT 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `zoomcamp-3.ny_taxi.green_tripdata_non_partitioned` AS
SELECT *
FROM `zoomcamp-3.ny_taxi.external_green_tripdata`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `zoomcamp-3.ny_taxi.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT *
FROM `zoomcamp-3.ny_taxi.external_green_tripdata`;


-- Impact of partition
-- Scanning ~22.43 MB of data
SELECT DISTINCT(VendorID)
FROM `zoomcamp-3.ny_taxi.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime)
  BETWEEN '2020-06-01' AND '2020-06-30'


-- Scanning ~793.3 KB MB of DATA
SELECT DISTINCT(VendorID)
FROM `zoomcamp-3.ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime)
  BETWEEN '2020-06-01' AND '2020-06-30'


-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `zoomcamp-3.ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitioned'
ORDER BY total_rows DESC;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `zoomcamp-3.ny_taxi.green_tripdata_partitioned_clustered`

PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS 
SELECT 
  CAST(VendorID AS INT64) AS VendorID,
  * 
EXCEPT (VendorID)
FROM `zoomcamp-3.ny_taxi.external_green_tripdata`


-- Query scans 22.43 MB
SELECT COUNT(*) AS trips
FROM `zoomcamp-3.ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime)
    BETWEEN '2020-01-01' AND '2020-12-31'
  AND VendorID= 1;


  -- Query scans 22.43 MB
SELECT COUNT(*) AS trips
FROM `zoomcamp-3.ny_taxi.green_tripdata_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime)
    BETWEEN '2020-01-01' AND '2020-12-31'
  AND VendorID = 1