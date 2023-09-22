import unittest
from unittest.mock import Mock, patch

from upstream.logic import bronze


@patch('requests.get')
class TestGetMessages(unittest.TestCase):
    def test_url_called(self, mock_get):
        """
        Test the get_messages function. It mocks the requests.get function to simulate API responses, and uses that
        mock response in the get_messages function. The test asserts that if a valid URL is passed into get_messages,
        it will return a list of messages.
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


if __name__ == '__main__':
    unittest.main()
