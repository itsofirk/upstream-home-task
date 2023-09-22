import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pandas as pd
import requests.models

from upstream.common.exceptions import ApiError, DataLakeError
from upstream.logic import bronze


class TestGetMessages(unittest.TestCase):
    """
    Test the get_messages function. It mocks the requests.get function to simulate API responses
    """

    @patch('requests.get')
    def test_url_called(self, mock_get):
        """
        Asserts that if a valid URL is passed into get_messages, it will return a list of messages.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'id': 1, 'data': 'example_data'}]

        mock_get.return_value = mock_response

        url = 'http://example.com'
        amount = 10

        result = bronze.get_messages(url, amount)
        self.assertEqual(result, [{'id': 1, 'data': 'example_data'}])
        mock_get.assert_called_once_with(url, params={'amount': amount})

    @patch('requests.get')
    def test_bad_url(self, mock_get):
        """
        Asserts that if the passed URL responds with a bad status code it will raise an exception.
        """
        mock_response = Mock()
        mock_response.status_code = 400

        mock_get.return_value = mock_response

        url = 'http://example.com'
        amount = 10

        with self.assertRaises(ApiError):
            bronze.get_messages(url, amount)

    @patch('requests.get')
    def test_bad_response(self, mock_get):
        """
        Asserts that if the API returns a malformed response, it will raise an exception.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = 'BAD JSON'

        def raise_json_decode_error():
            raise requests.exceptions.JSONDecodeError("Expecting value", "", 0)

        # Set the json method of the mock response to raise the exception
        mock_response.json.side_effect = raise_json_decode_error
        mock_get.return_value = mock_response

        url = 'http://example.com'
        amount = 10

        with self.assertRaises(ApiError):
            bronze.get_messages(url, amount)


class TestExportParquet(unittest.TestCase):
    """
    Test the export_parquet function.
    """

    @patch('pyarrow.parquet.write_to_dataset')
    def test_export_parquet_success(self, mock_write_to_dataset):
        data = {'timestamp': ['2023-09-22 12:00:00', '2023-09-22 13:00:00'],
                'data': [1, 2]}
        df = pd.DataFrame(data)

        bronze.export_parquet(df, 'bronze_dir')

        expected_table = pa.Table.from_pandas(df)
        mock_write_to_dataset.assert_called_once_with(expected_table, root_path='bronze_dir',
                                                      partition_cols=['date', 'hour'])

    @patch('pyarrow.parquet.write_to_dataset')
    @patch('upstream.datalake.is_empty')
    def test_export_parquet_success(self, mock_is_empty, mock_write_to_dataset):
        mock_is_empty.return_value = False

        data = {'timestamp': ['2023-09-22 12:00:00'], 'data': [1]}
        df = pd.DataFrame(data)

        with self.assertRaises(DataLakeError):
            bronze.export_parquet(df, 'bronze_dir')

        mock_write_to_dataset.assert_not_called()


if __name__ == '__main__':
    unittest.main()
