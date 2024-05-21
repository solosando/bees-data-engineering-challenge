import unittest
from unittest.mock import patch, MagicMock
from resources.extract import fetch_data_from_api, store_data_on_s3


class TestFetchAndStoreFunctions(unittest.TestCase):

    @patch("your_module.requests.get")
    def test_fetch_data_from_api_success(self, mock_get):
        # Mock the requests.get() function
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"key": "value"}
        mock_get.return_value = mock_response

        # Call the function
        data = fetch_data_from_api("http://example.com/api")

        # Assertions
        self.assertEqual(data, {"key": "value"})

    @patch("your_module.boto3.client")
    def test_store_data_on_s3_success(self, mock_s3_client):
        # Mock the boto3.client() function
        mock_client = MagicMock()
        mock_s3_client.return_value = mock_client

        # Call the function
        data = {"key": "value"}
        store_data_on_s3(data, "test_bucket", "test_key.json")

        # Assertions
        mock_client.put_object.assert_called_once_with(
            Body=b'{"key": "value"}', Bucket="test_bucket", Key="test_key.json"
        )


if __name__ == "__main__":
    unittest.main()
