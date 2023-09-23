import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from upstream.infrastructure import datalake
from upstream.common.exceptions import DataLakeError


class TestExportParquet(unittest.TestCase):
    """
    Test the export_parquet function.
    """
    @patch('pyarrow.parquet.write_to_dataset')
    @patch('upstream.infrastructure.datalake.is_empty')
    def test_export_parquet_success(self, mock_is_empty, mock_write_to_dataset):
        data = {'timestamp': ['2023-09-22 12:00:00', '2023-09-22 13:00:00'],
                'data': [1, 2]}
        df = pd.DataFrame(data)
        mock_is_empty.return_value = True
        datalake.export_parquet(df, 'bronze_dir', partition_cols=['date', 'hour'])

        expected_table = pa.Table.from_pandas(df)
        mock_write_to_dataset.assert_called_once_with(expected_table, root_path='bronze_dir',
                                                      partition_cols=['date', 'hour'])

    @patch('pyarrow.parquet.write_to_dataset')
    @patch('upstream.infrastructure.datalake.is_empty')
    def test_bronze_dir_not_empty(self, mock_is_empty, mock_write_to_dataset):
        mock_is_empty.return_value = False

        data = {'timestamp': ['2023-09-22 12:00:00'], 'data': [1]}
        df = pd.DataFrame(data)

        with self.assertRaises(DataLakeError):
            datalake.export_parquet(df, 'bronze_dir')

        mock_write_to_dataset.assert_not_called()

    @patch('upstream.infrastructure.datalake.is_empty')
    def test_missing_field_in_schema(self, mock_is_empty):
        data = {'timestamp': ['2023-09-22 12:00:00', '2023-09-22 13:00:00'],
                'data': [1, 2]}
        df = pd.DataFrame(data)
        mock_is_empty.return_value = True

        with self.assertRaises(KeyError):
            datalake.export_parquet(df, '/dev/null', partition_cols=['not_in_schema'])


if __name__ == '__main__':
    unittest.main()
