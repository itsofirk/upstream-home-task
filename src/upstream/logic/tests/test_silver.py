import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

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
        assert_frame_equal(expected, output)


class TestStandardizeGearPosition(unittest.TestCase):
    """
    Test the standardize_gear_position function.
    """
    def test_standardize_gear_position(self):
        df = pd.DataFrame({'gearPosition': ['1', None, 3, 'REVERSE', 'Unknown']})
        expected = pd.DataFrame({'gearPosition': [1, pd.NA, pd.NA, -1, pd.NA]}, dtype='Int64')
        gear_mapping = {
            '1': 1,
            None: pd.NA,
            'REVERSE': -1
        }
        output = silver.standardize_gear_position(df, gear_mapping)
        assert_frame_equal(expected, output)


if __name__ == '__main__':
    unittest.main()
