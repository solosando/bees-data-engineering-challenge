import os
import logging
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row

# Assuming the script is named transform and the functions are imported from there
from transform import transform_json_to_parquet, main


class TestTransform(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "fake_access_key",
            "AWS_SECRET_ACCESS_KEY": "fake_secret_key",
        },
    )
    @patch("transform.Variable.get")
    @patch("transform.SparkSession")
    @patch("transform.logging")
    def test_transform_json_to_parquet(
        self, mock_logging, mock_spark_session, mock_variable_get
    ):
        # Mocking the Airflow Variable.get method
        mock_variable_get.side_effect = lambda key: {
            "s3_bucket_1": "input_bucket",
            "key": "input_key",
            "s3_bucket_2": "output_bucket",
        }[key]

        # Create a mock Spark session
        spark = SparkSession.builder.appName("test").getOrCreate()
        mock_spark_session.builder.appName.return_value = mock_spark_session.builder
        mock_spark_session.builder.config.return_value = mock_spark_session.builder
        mock_spark_session.builder.getOrCreate.return_value = spark

        # Create sample JSON data
        data = [
            Row(name="Brewery A", country="USA", location="NY"),
            Row(name="Brewery B", country="Canada", location="Toronto"),
            Row(name="Brewery C", country="USA", location="CA"),
        ]
        df = spark.createDataFrame(data)

        # Mock the read and write methods
        mock_read = MagicMock(return_value=df)
        spark.read.json = mock_read

        # Mock the write method to track what gets written
        mock_write = MagicMock()
        DataFrame.write = mock_write

        # Call the function
        transform_json_to_parquet(
            "input_bucket", "input_key", "output_bucket", "output_key"
        )

        # Assertions for the read call
        mock_read.assert_called_once_with("s3a://input_bucket/input_key")

        # Assertions for the write call
        self.assertEqual(mock_write.partitionBy.call_count, 1)
        mock_write.partitionBy.assert_called_once_with("country")
        mock_write.partitionBy().parquet.assert_called_once_with(
            "s3a://output_bucket/output_key"
        )

        # Assertions for logging
        mock_logging.info.assert_any_call(
            "Read JSON file from s3://input_bucket/input_key"
        )
        mock_logging.info.assert_any_call(
            "Wrote Parquet file to s3://output_bucket/output_key"
        )

    @patch("transform.Variable.get")
    @patch("transform.transform_json_to_parquet")
    def test_main(self, mock_transform_json_to_parquet, mock_variable_get):
        # Mocking the Airflow Variable.get method
        mock_variable_get.side_effect = lambda key: {
            "s3_bucket_1": "input_bucket",
            "key": "input_key",
            "s3_bucket_2": "output_bucket",
        }[key]

        main()

        # Assertions for the transform_json_to_parquet call
        mock_transform_json_to_parquet.assert_called_once_with(
            "input_bucket", "input_key.json", "output_bucket", "input_key.parquet"
        )


if __name__ == "__main__":
    unittest.main()
