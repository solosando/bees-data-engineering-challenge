import logging
from pyspark.sql import SparkSession

input_bucket = "ariel-lake-bronze-layer"
input_key = "breweries.json"
output_bucket = "ariel-lake-silver-layer"
output_key = "breweries.parquet"

# Initialize Spark session
spark = SparkSession.builder.appName("TransformJSONToParquet").getOrCreate()

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
