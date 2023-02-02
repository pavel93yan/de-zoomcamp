from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")

GcsBucket(bucket="dtc_data_lake_de-project-375820",
          gcp_credentials=gcp_credentials_block
          ).save("zoom-gcs")