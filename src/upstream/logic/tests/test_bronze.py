import unittest
from unittest.mock import Mock, patch

import requests.models

from upstream.common.exceptions import ApiError
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


if __name__ == '__main__':
    unittest.main()
