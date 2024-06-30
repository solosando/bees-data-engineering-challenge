import sys
import boto3
import requests
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
glue_client = boto3.client("glue")
args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
workflow_name = args["WORKFLOW_NAME"]
workflow_run_id = args["WORKFLOW_RUN_ID"]
workflow_params = glue_client.get_workflow_run_properties(
    Name=workflow_name, RunId=workflow_run_id
)["RunProperties"]
api_url = workflow_params["API_URL"]
bucket = workflow_params["BRONZE_BUCKET"]
s3_object_key = workflow_params["OBJ_KEY"]


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def store_data_on_s3(data, s3_bucket_name, s3_object_key):
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Body=json.dumps(data).encode("utf-8"),
            Bucket=s3_bucket_name,
            Key=s3_object_key + ".json",
        )
        print(f"Data stored in s3://{s3_bucket_name}/{s3_object_key}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Error uploading data to S3: {e}")


def main():

    # Fetch data from API
    data = fetch_data_from_api(api_url)
    if data is not None:
        store_data_on_s3(data, bucket, s3_object_key)


if __name__ == "__main__":
    main()
    job.commit()
