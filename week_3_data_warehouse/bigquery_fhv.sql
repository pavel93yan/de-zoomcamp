CREATE OR REPLACE EXTERNAL TABLE `de-project-375820.trips_data_all.external_fhv_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_de-project-375820/fhv/fhv_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `de-project-375820.trips_data_all.fhv_tripdata_non_partitoned` AS
SELECT * FROM `de-project-375820.trips_data_all.external_fhv_taxi`;

SELECT COUNT(*) FROM `de-project-375820.trips_data_all.fhv_tripdata_non_partitoned`;

SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-project-375820.trips_data_all.fhv_tripdata_non_partitoned`;
SELECT COUNT(DISTINCT affiliated_base_number) FROM `de-project-375820.trips_data_all.external_fhv_taxi`;

SELECT COUNT(*) FROM `de-project-375820.trips_data_all.fhv_tripdata_non_partitoned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

CREATE OR REPLACE TABLE `de-project-375820.trips_data_all.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `de-project-375820.trips_data_all.external_fhv_taxi`;

SELECT DISTINCT affiliated_base_number
FROM `de-project-375820.trips_data_all.fhv_tripdata_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT affiliated_base_number
FROM `de-project-375820.trips_data_all.fhv_tripdata_non_partitoned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';