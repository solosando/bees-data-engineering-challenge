import sys
import boto3
import logging
import time
from pyspark.sql import SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get job parameters
glue_client = boto3.client("glue")
input_bucket = "ariel-lake-silver-layer"
output_bucket = "ariel-lake-gold-layer"
input_key = "breweries.parquet"
crawler_name = "view_crawler"

spark = SparkSession.builder.appName("CreateView").getOrCreate()


def create_aggregate_views(input_bucket: str, output_bucket: str):
    try:
        # Read the data from S3
        df = spark.read.parquet(f"s3a://{input_bucket}/{input_key}")

        # Create the aggregate view
        view = (
            df.groupBy("brewery_type", "country")
            .count()
            .withColumnRenamed("count", "record_count")
        )

        # Save the aggregate view to S3
        view.write.mode("overwrite").parquet(f"s3a://{output_bucket}/views.parquet")

        logging.info(f"Successfully wrote aggregate view to s3://{output_bucket}/")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise  # Re-raise the exception to propagate it


def start_crawler(crawler_name: str):
    try:
        # Start the Glue Crawler
        glue_client.start_crawler(Name=crawler_name)
        logging.info(f"Started Crawler: {crawler_name}")

        # Monitor the Crawler's status
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response["Crawler"]["State"]

            if crawler_state == "READY":
                logging.info(f"Crawler {crawler_name} has completed.")
                break
            else:
                logging.info(
                    f"Crawler {crawler_name} is in state: {crawler_state}. Waiting..."
                )
                time.sleep(30)  # Wait for 30 seconds before checking the status again

        logging.info("Crawler run completed successfully.")

    except Exception as e:
        logging.error(
            f"An error occurred while running the crawler: {e}", exc_info=True
        )
        raise  # Re-raise the exception to propagate it


def main():
    create_aggregate_views(input_bucket, output_bucket)
    start_crawler(crawler_name)


if __name__ == "__main__":
    main()
