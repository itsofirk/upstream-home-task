import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from upstream.logic.silver import clean_data, standardize_gear_position


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
        output = clean_data(df, null_filtering_columns=['vin'])
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
        output = standardize_gear_position(df, gear_mapping)
        assert_frame_equal(expected, output)


if __name__ == '__main__':
    unittest.main()
