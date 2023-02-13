-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-project-375820.trips_data_all.external_green_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_de-project-375820/green/green_tripdata_2019-*.parquet', 'gs://dtc_data_lake_de-project-375820/green/green_tripdata_2020-*.parquet']
);


-- Check green trip data
SELECT * FROM `de-project-375820.trips_data_all.external_green_taxi` limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `de-project-375820.trips_data_all.green_tripdata_non_partitoned` AS
SELECT * FROM `de-project-375820.trips_data_all.external_green_taxi`;


-- Create a  partitioned table from external table
CREATE OR REPLACE TABLE `de-project-375820.trips_data_all.green_tripdata_partitoned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `de-project-375820.trips_data_all.external_green_taxi`;

-- Impact of partition
-- Scanning 78.9 MB  of data
SELECT DISTINCT(VendorID)
FROM `de-project-375820.trips_data_all.green_tripdata_non_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning 4.94 MB of DATA
SELECT DISTINCT(VendorID)
FROM `de-project-375820.trips_data_all.green_tripdata_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `de-project-375820.trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-project-375820.trips_data_all.green_tripdata_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-project-375820.trips_data_all.external_green_taxi`;

-- Query scans 49.24 MB
SELECT count(*) as trips
FROM `de-project-375820.trips_data_all.green_tripdata_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID='1';

-- Query scans 48.19 MB
SELECT count(*) as trips
FROM `de-project-375820.trips_data_all.green_tripdata_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID='1';