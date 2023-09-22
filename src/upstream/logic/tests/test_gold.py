import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from upstream.logic import gold


class TestVinReport(unittest.TestCase):
    def test_vin_report_success(self):
        df = pd.DataFrame({
            'timestamp': ['2023-09-22 15:00:00', '2023-09-22 12:00:00',
                          '2023-09-22 13:00:00', '2023-09-22 14:00:00',
                          '2023-09-22 13:00:00'],
            'vin': ['A', 'A', 'B', 'B', 'A'],
            'frontLeftDoorState': ['LOCKED1', None, 'LOCKED', 'UNLOCKED', 'LOCKED'],
            'wipersState': [False, False, False, True, True]
        })
        expected = pd.DataFrame({
            'vin': ['A', 'B'],
            'timestamp': ['2023-09-22 15:00:00', '2023-09-22 14:00:00'],
            'frontLeftDoorState': ['LOCKED1', 'UNLOCKED'],
            'wipersState': [False, True]
        })

        output = gold.generate_vin_last_state_report(df)
        assert_frame_equal(expected, output)


class TestTop10Fastest(unittest.TestCase):
    def test_top_10_fastest_report(self):
        """
        Test the generate_top_10_fastest_vehicles function. The test creates a dataframe with 20 rows, 15 of which
        have the same hour, and 5 of which have a different hour. The first 15 rows are given velocities from 10 to
        150, and the last 5 are given velocities from 160 to 200. We then create an expected dataframe with only 15
        rows: The first five should be all the rows from the different hour, and the last 10 should be the last 15
        rows from the same hour
        """
        df = pd.DataFrame({
            'date': ['same-date'] * 20,
            'hour': ['same-hour'] * 15 + ['different-hour'] * 5,
            'vin': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
                    'A', 'B', 'C', 'D', 'E'],
            'velocity': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150,
                         160, 170, 180, 190, 200]
        })
        expected = pd.DataFrame({
            'date': ['same-date'] * 15,
            'hour': ['different-hour'] * 5 + ['same-hour'] * 10,
            'vin': ['E', 'D', 'C', 'B', 'A',
                    'O', 'N', 'M', 'L', 'K', 'J', 'I', 'H', 'G', 'F'],
            'velocity': [200, 190, 180, 170, 160,
                         150, 140, 130, 120, 110, 100, 90, 80, 70, 60]
        })

        output = gold.generate_top_10_fastest_vehicles_report(df)
        assert_frame_equal(expected, output)


if __name__ == '__main__':
    unittest.main()
