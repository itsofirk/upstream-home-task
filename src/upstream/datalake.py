"""
Set up the filesystem and create the necessary directories that would act as the Data Lake.
"""

import os
import logging
from pathlib import Path

from upstream.common.exceptions import FilesystemError

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
            raise FilesystemError(f"expected {data_dir} to be empty")
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

