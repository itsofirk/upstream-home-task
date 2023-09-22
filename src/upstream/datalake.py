"""
Set up the filesystem and create the necessary directories that would act as the Data Lake.
"""

import os
import logging
from pathlib import Path
from pyarrow import Table, parquet as pq

from upstream.common.exceptions import DataLakeError

logger = logging.getLogger(__name__)


def set_up_local_data_lake(root_path, bronze='bronze', silver='silver', gold='gold'):
    """
    Create the directories for the local data lake.
    This function creates the following directory structure under the specified root directory
    ...root_path/ { bronze/, silver/, gold/ }
    """
    logger.info("Set up environment...")

    root_folder = Path(root_path)
    logger.debug(f"Creating main data folder at {root_path}")
    if root_folder.exists() and root_folder.is_dir():
        logger.debug("Main data folder already exists")
    else:
        root_folder.mkdir()

    logger.debug("Creating nested data folders")
    for data_dir in [bronze, silver, gold]:
        if not is_empty(root_folder / data_dir):
            raise DataLakeError(f"expected {data_dir} to be empty")
        (root_folder / data_dir).mkdir(exist_ok=True)
    logger.info("Data Lake directories created.")


def is_empty(directory):
    """
    Checks if a directory is empty.
    """
    return not path_exists(directory) or not list_dir(directory)


def list_dir(directory):
    """
    Takes a directory path and returns the list of files in that directory.
    """
    return os.listdir(directory)


def path_exists(path):
    """
    Takes a directory path and returns True if path refers to an existing path, False otherwise.
    """
    return os.path.exists(path)


def export_parquet(df, path, partition_cols=None):
    """
    export_parquet takes a dataframe and writes it to the bronze directory as parquet files partitioned by date and
    hour extracted from the timestamp column.

    :param df: The dataframe to be exported
    :param path: Specify the directory where the parquet file will be stored
    :param partition_cols: Specify the column names by which to partition the dataset
    :return: The directory path to export the parquet files onto.
    """
    logger.info("Exporting table to path...")
    table = Table.from_pandas(df)
    if not is_empty(path):
        raise DataLakeError("Provided directory is not empty.")
    pq.write_to_dataset(table, root_path=path, partition_cols=partition_cols)
