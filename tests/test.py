import unittest
from unittest.mock import patch, Mock
import json
import requests
import boto3
from boto3.exceptions import S3UploadFailedError

# Importing functions from jobs.extract
from jobs.extract import fetch_data_from_api, store_data_on_s3, main


class TestExtractJob(unittest.TestCase):

    @patch("jobs.extract.requests.get")
    def test_fetch_data_from_api_success(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"key": "value"}
        mock_get.return_value = mock_response

        result = fetch_data_from_api("https://api.openbrewerydb.org/breweries")
        self.assertEqual(result, {"key": "value"})

    @patch("jobs.extract.requests.get")
    def test_fetch_data_from_api_failure(self, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException("Error")
        result = fetch_data_from_api("https://api.openbrewerydb.org/breweries")
        self.assertIsNone(result)

    @patch("jobs.extract.boto3.client")
    def test_store_data_on_s3_success(self, mock_boto3_client):
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client

        data = {"key": "value"}
        store_data_on_s3(data, "test-bucket", "test-key")
        mock_s3_client.put_object.assert_called_once_with(
            Body=json.dumps(data).encode("utf-8"),
            Bucket="tariel-lake-bronze-layer",
            Key="breweries.json",
        )

    @patch("jobs.extract.boto3.client")
    def test_store_data_on_s3_failure(self, mock_boto3_client):
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        mock_s3_client.put_object.side_effect = S3UploadFailedError("Error")

        data = {"key": "value"}
        store_data_on_s3(data, "tariel-lake-bronze-layer", "breweries")

    @patch("jobs.extract.fetch_data_from_api")
    @patch("jobs.extract.store_data_on_s3")
    def test_main(self, mock_store_data_on_s3, mock_fetch_data_from_api):
        mock_fetch_data_from_api.return_value = {"key": "value"}

        # Simulate calling main function
        main()

        # Check if fetch_data_from_api was called with correct argument
        mock_fetch_data_from_api.assert_called_once_with(
            "https://api.openbrewerydb.org/breweries"
        )

        # Check if store_data_on_s3 was called with correct arguments
        mock_store_data_on_s3.assert_called_once_with(
            {"key": "value"}, "ariel-lake-bronze-layer", "test-key"
        )


if __name__ == "__main__":
    unittest.main()
