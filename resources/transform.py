import os
import logging
from pyspark.sql import SparkSession
from airflow.models import Variable

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize SparkSession
spark = (
    SparkSession.builder.appName("visualizer")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    .getOrCreate()
)

# Create Spark session
spark = SparkSession.builder.appName("transform").config(conf=spark_conf).getOrCreate()

# Create Spark context
sc = spark.sparkContext


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
    # Retrieve variables from Airflow
    input_bucket = Variable.get("s3_bucket_1")
    input_key = f"{Variable.get('s3_object_key')}.json"
    output_bucket = Variable.get("s3_bucket_2")
    output_key = f"{Variable.get('s3_object_key')}.parquet"

    transform_json_to_parquet(input_bucket, input_key, output_bucket, output_key)


if __name__ == "__main__":
    main()
    spark.stop()
