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


def create_aggregate_views(input_bucket: str, output_bucket: str):
    try:

        # Read the data from S3
        df = spark.read.parquet(f"s3a://{input_bucket}/*")

        # Create the aggregate view by_type
        by_type = (
            df.groupBy("brewery_type")
            .count()
            .withColumnRenamed("count", "record_count")
        )

        # Create the aggregate view by_location
        by_location = (
            df.groupBy("country").count().withColumnRenamed("count", "record_count")
        )

        # Save the aggregate views to S3
        by_type.write.mode("overwrite").parquet(
            f"s3a://{output_bucket}/types_of_breweries"
        )
        by_location.write.mode("overwrite").parquet(
            f"s3a://{output_bucket}/locations_of_breweries"
        )

    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    # Retrieve variables from Airflow
    input_bucket = Variable.get("s3_bucket_2")
    output_bucket = Variable.get("s3_bucket_3")

    create_aggregate_views(input_bucket, output_bucket)


if __name__ == "__main__":
    main()
    spark.stop()
