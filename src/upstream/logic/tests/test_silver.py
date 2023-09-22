import unittest
import pandas as pd

from upstream.logic import silver


class TestCleanData(unittest.TestCase):
    """
    Test the clean_data function.
    """
    def test_cleaning_success(self):
        df = pd.DataFrame({
            'vin': ['1', '2', None, '4'],
            'manufacturer': ['A', 'B', ' C', 'D ']
        })
        expected = pd.DataFrame({
            'vin': ['1', '2'],
            'manufacturer': ['A', 'B']
        })
        output = silver.clean_data(df)
        self.assertTrue(expected.equals(output))


if __name__ == '__main__':
    unittest.main()
