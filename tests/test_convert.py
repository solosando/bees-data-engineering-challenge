import unittest
from unittest.mock import patch, Mock
from pyspark.sql import SparkSession, DataFrame
from jobs.convert import (
    transform_json_to_parquet,
    input_bucket,
    input_key,
    output_bucket,
    output_key,
)


class TestTransformJSONToParquet(unittest.TestCase):

    @patch.object(SparkSession, "read")
    def test_transform_json_to_parquet(self, mock_read):
        # Create a mock DataFrame
        mock_df = Mock(spec=DataFrame)

        # Mock the return value of spark.read.json
        mock_read.json.return_value = mock_df

        # Mock the df.write and df.write.partitionBy().parquet methods
        mock_write_partition = Mock()
        mock_df.write.partitionBy.return_value = mock_write_partition
        mock_write_partition.parquet = Mock()

        # Call the function
        transform_json_to_parquet(input_bucket, input_key, output_bucket, output_key)

        # Assertions
        mock_read.json.assert_called_once_with(f"s3a://{input_bucket}/{input_key}")
        mock_df.write.partitionBy.assert_called_once_with("country")
        mock_write_partition.parquet.assert_called_once_with(
            f"s3a://{output_bucket}/{output_key}"
        )


if __name__ == "__main__":
    unittest.main()
