import sys
import boto3
import logging
from pyspark.sql import SparkSession
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
input_bucket = workflow_params["BRONZE_BUCKET"]
input_key = workflow_params["OBJ_KEY"] + ".json"
output_bucket = workflow_params["SILVER_BUCKET"]
output_key = workflow_params["OBJ_KEY"] + ".parquet"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set up logging
logging.basicConfig(level=logging.INFO)


def transform_json_to_parquet(
    input_bucket: str, input_key: str, output_bucket: str, output_key: str
) -> None:
    try:
        # Read JSON file from S3
        df = spark.read.json(f"s3a://{input_bucket}/{input_key}")
        logging.info(f"Read JSON file from s3://{input_bucket}/{input_key}")

        # Write Parquet file to S3
        df.write.partitionBy("country").parquet(f"s3a://{output_bucket}/{output_key}")
        logging.info(f"Wrote Parquet file to s3://{output_bucket}/{output_key}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)


def main() -> None:
    transform_json_to_parquet(input_bucket, input_key, output_bucket, output_key)


if __name__ == "__main__":
    main()
    job.commit()
