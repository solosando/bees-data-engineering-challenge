import unittest
from unittest.mock import patch, Mock
import boto3
import requests
import json
from jobs.extract import (
    fetch_data_from_api,
    store_data_on_s3,
    main,
)

api_url = "https://api.openbrewerydb.org/breweries"
bucket = "ariel-lake-bronze-layer"
s3_object_key = "breweries"


class TestMyModule(unittest.TestCase):

    @patch("requests.get")
    def test_fetch_data_from_api(self, mock_get):
        # Define the mock response
        mock_response = Mock()
        expected_data = [{"id": "1", "name": "Test Brewery"}]
        mock_response.json.return_value = expected_data
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call the function
        api_url = "https://api.openbrewerydb.org/breweries"
        data = fetch_data_from_api(api_url)

        # Assertions
        self.assertEqual(data, expected_data)
        mock_get.assert_called_once_with(api_url)
        mock_response.raise_for_status.assert_called_once()

    @patch("boto3.client")
    def test_store_data_on_s3(self, mock_boto_client):
        # Mock the boto3 client and its method
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client

        # Call the function
        data = [{"id": "1", "name": "Test Brewery"}]
        bucket = "test-bucket"
        object_key = "test-key"
        store_data_on_s3(data, bucket, object_key)

        # Assertions
        mock_boto_client.assert_called_once_with("s3")
        mock_s3_client.put_object.assert_called_once_with(
            Body=json.dumps(data).encode("utf-8"),
            Bucket=bucket,
            Key=object_key + ".json",
        )

    @patch("jobs.extract.fetch_data_from_api")
    @patch("jobs.extract.store_data_on_s3")
    def test_main(self, mock_store_data_on_s3, mock_fetch_data_from_api):
        # Mock the fetch_data_from_api to return test data
        mock_fetch_data_from_api.return_value = [{"id": "1", "name": "Test Brewery"}]

        # Call the main function
        main()

        # Assertions
        mock_fetch_data_from_api.assert_called_once_with(api_url)
        mock_store_data_on_s3.assert_called_once_with(
            [{"id": "1", "name": "Test Brewery"}], bucket, s3_object_key
        )


if __name__ == "__main__":
    unittest.main()
