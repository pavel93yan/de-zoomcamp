import os
import pandas as pd
from google.cloud import storage
import pyarrow
"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_de-project-375820")

table_schema_green = {
        'VendorID': 'str',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'Int64',
        'trip_type': 'Int64',
        'congestion_surcharge': 'float64'
}

table_schema_fhv = {
    'dispatching_base_num': 'str',
    'PUlocationID': 'Int64',
    'DOlocationID': 'Int64',
    'SR_Flag': 'Int64',
    'Affiliated_base_number': 'str'
}


date_cols_fhv = ['pickup_datetime', 'dropOff_datetime']
date_cols_green = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 1 * 1024 * 256  # 0.25 MB
    storage.blob._DEFAULT_CHUNKSIZE = 1 * 1024 * 256  # 0.25 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0' + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'
        # file_name = f"{service}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{file_name}"
        # download it using requests via a pandas df
        # request_url = init_url + file_name
        # r = requests.get(request_url)
        # pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
        df = pd.read_csv(dataset_url, dtype=table_schema_fhv,
                         parse_dates=date_cols_fhv)
        print(f"Fetched: {file_name}")

        # read it back into a parquet file
        # df = pd.read_csv(file_name)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

