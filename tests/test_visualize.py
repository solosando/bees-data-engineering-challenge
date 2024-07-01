import unittest
from unittest.mock import MagicMock, patch
from jobs.visualize import create_aggregate_views, start_crawler


class TestCreateAggregateViews(unittest.TestCase):

    @patch("jobs.visualize.spark")
    def test_create_aggregate_views_success(self, mock_spark):
        # Mock the Spark DataFrame and its methods
        mock_df = MagicMock()
        mock_spark.read.parquet.return_value = mock_df
        mock_grouped_df = MagicMock()
        mock_df.groupBy.return_value = mock_grouped_df
        mock_agg_df = MagicMock()
        mock_grouped_df.count.return_value.withColumnRenamed.return_value = mock_agg_df

        # Call the function
        create_aggregate_views("mock_input_bucket", "mock_output_bucket")

        # Assertions to check if the methods were called as expected
        mock_spark.read.parquet.assert_called_once_with(
            "s3a://mock_input_bucket/breweries.parquet"
        )
        mock_df.groupBy.assert_called_once_with("brewery_type", "country")
        mock_grouped_df.count.return_value.withColumnRenamed.assert_called_once_with(
            "count", "record_count"
        )
        mock_agg_df.write.mode.return_value.parquet.assert_called_once_with(
            "s3a://mock_output_bucket/views.parquet"
        )

    @patch("jobs.visualize.spark")
    def test_create_aggregate_views_failure(self, mock_spark):
        # Mock the Spark read.parquet method to raise an exception
        mock_spark.read.parquet.side_effect = Exception("Test exception")

        # Call the function and assert the exception is raised
        with self.assertRaises(Exception) as context:
            create_aggregate_views("mock_input_bucket", "mock_output_bucket")

        # Optionally, check the exception message
        self.assertEqual(str(context.exception), "Test exception")


class TestStartCrawler(unittest.TestCase):

    @patch("jobs.visualize.glue_client")
    @patch("time.sleep", return_value=None)  # Mock sleep to avoid waiting in the test
    def test_start_crawler_success(self, mock_sleep, mock_glue_client):
        # Mock the Glue client responses
        mock_glue_client.get_crawler.side_effect = [
            {"Crawler": {"State": "RUNNING"}},
            {"Crawler": {"State": "READY"}},
        ]

        # Call the function
        start_crawler("mock_crawler_name")

        # Assertions to check if the
