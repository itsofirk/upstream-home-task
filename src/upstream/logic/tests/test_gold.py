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


if __name__ == '__main__':
    unittest.main()
