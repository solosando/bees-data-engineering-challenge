import os
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Assuming the module is named visualizer and the functions are imported from there
from visualizer import create_aggregate_views, main


class TestVisualizer(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "fake_access_key",
            "AWS_SECRET_ACCESS_KEY": "fake_secret_key",
        },
    )
    @patch("visualizer.Variable.get")
    @patch("visualizer.SparkSession")
    def test_create_aggregate_views(self, mock_spark_session, mock_variable_get):
        # Mocking the Airflow Variable.get method
        mock_variable_get.side_effect = lambda key: {
            "s3_bucket_2": "input_bucket",
            "s3_bucket_3": "output_bucket",
        }[key]

        # Create a mock Spark session
        spark = SparkSession.builder.appName("test").getOrCreate()
        mock_spark_session.builder.appName.return_value = mock_spark_session.builder
        mock_spark_session.builder.config.return_value = mock_spark_session.builder
        mock_spark_session.builder.getOrCreate.return_value = spark

        # Create sample data
        data = [
            Row(type="brewery", location="NY", other_field="A"),
            Row(type="brewery", location="CA", other_field="B"),
            Row(type="pub", location="NY", other_field="C"),
        ]
        df = spark.createDataFrame(data)

        # Mock the read and write methods
        mock_read = MagicMock(return_value=df)
        spark.read.parquet = mock_read

        # Mock the write method to track what gets written
        mock_write = MagicMock()
        DataFrame.write = mock_write

        create_aggregate_views("input_bucket", "output_bucket")

        # Assertions for the read call
        mock_read.assert_called_once_with("s3a://input_bucket/*")

        # Assertions for the write call
        self.assertEqual(mock_write.mode.call_count, 2)
        mock_write.mode.assert_any_call("overwrite")

        # Check if the correct DataFrame is written to the correct path
        args, _ = mock_write.mode().parquet.call_args_list[0]
        self.assertIn("s3a://output_bucket/types_of_breweries", args)

        args, _ = mock_write.mode().parquet.call_args_list[1]
        self.assertIn("s3a://output_bucket/locations_of_breweries", args)

    @patch("visualizer.Variable.get")
    @patch("visualizer.create_aggregate_views")
    def test_main(self, mock_create_aggregate_views, mock_variable_get):
        # Mocking the Airflow Variable.get method
        mock_variable_get.side_effect = lambda key: {
            "s3_bucket_2": "input_bucket",
            "s3_bucket_3": "output_bucket",
        }[key]

        main()

        # Assertions for the create_aggregate_views call
        mock_create_aggregate_views.assert_called_once_with(
            "input_bucket", "output_bucket"
        )


if __name__ == "__main__":
    unittest.main()
