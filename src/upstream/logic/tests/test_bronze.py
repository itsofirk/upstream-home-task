import unittest
from unittest.mock import Mock, patch

import pandas as pd
from pandas.testing import assert_frame_equal
import requests.models

from upstream.common.exceptions import ApiError
from upstream.logic.bronze import get_messages, parse_messages


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

        result = get_messages(url, amount)
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
            get_messages(url, amount)

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
            get_messages(url, amount)


class TestParseMessages(unittest.TestCase):
    def test_parse_messages(self):
        messages = [
            {'timestamp': 1631234567890, 'data': 'message 1'},
            {'timestamp': 1631234567900, 'data': 'message 2'},
            {'timestamp': 1631234567910, 'data': 'message 3'}
        ]
        
        expected_result = pd.DataFrame({
            'timestamp': pd.to_datetime([1631234567890, 1631234567900, 1631234567910], unit='ms'),
            'data': ['message 1', 'message 2', 'message 3'],
            'date': [pd.Timestamp('2021-09-10').date(), pd.Timestamp('2021-09-10').date(), pd.Timestamp('2021-09-10').date()],
            'hour': [0, 0, 0]
        })
        
        result = parse_messages(messages)
        assert_frame_equal(result, expected_result, check_dtype=False)


if __name__ == '__main__':
    unittest.main()
