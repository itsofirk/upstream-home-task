"""
silver stage logic
"""
import logging
import numpy as np
import pandas as pd
from upstream.infrastructure import datalake

logger = logging.getLogger(__name__)
GEAR_POSITION_MAPPING = {'NEUTRAL': 0, 'REVERSE': -1, None: np.nan,
                         '-1': -1, '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6}


def load_bronze(bronze_dir: str) -> pd.DataFrame:
    logger.debug(f"Loading bronze data from {bronze_dir}")
    return datalake.load_parquet(bronze_dir)


def clean_data(df: pd.DataFrame, null_filtering_columns) -> pd.DataFrame:
    df = filter_null_values(df, null_filtering_columns)
    df = remove_rows_with_bad_manufacturer(df)
    return df


def filter_null_values(df, columns):
    """
    Remove rows from the dataframe that have a null value for the 'vin' column.
    """
    return df.dropna(subset=columns)


def remove_rows_with_bad_manufacturer(df):
    """
    Remove rows from the dataframe that have a manufacturer with leading or trailing whitespace.
    """
    return df[df.manufacturer.str.strip() == df.manufacturer]


def standardize_gear_position(df: pd.DataFrame, gear_mapping) -> pd.DataFrame:
    """
    Takes a dataframe as input and returns the same dataframe with the gearPosition column standardized converted to
    integers.
    If a value is not found in the gear_mapping, it will be mapped to NaN.
    """
    df['gearPosition'] = df.gearPosition.map(gear_mapping).astype('Int64')
    return df


def silver(bronze_dir: str, silver_dir: str, null_filtering_columns=None, gear_position_mapping=None):
    """
    The silver function takes the bronze data and cleans it up
    :param bronze_dir: Specify the directory of the bronze data
    :param silver_dir: Specify the directory where the silver data will be stored
    :param null_filtering_columns: Optional. Specify columns to filter out null values from
    :param gear_position_mapping: Optional. Map the gear position to a standard value
    """
    if null_filtering_columns is None:
        null_filtering_columns = []
    if gear_position_mapping is None:
        gear_position_mapping = GEAR_POSITION_MAPPING

    logger.info("Starting the silver stage...")
    logger.debug("Loading bronze data...")
    bronze = load_bronze(bronze_dir)
    logger.debug("Cleaning and Filtering bronze data...")
    df = clean_data(bronze, null_filtering_columns)
    logger.debug("Standardizing gear position...")
    df = standardize_gear_position(df, gear_position_mapping)
    logger.debug("Exporting data to silver_dir...")
    datalake.export_parquet(df, silver_dir, partition_cols=['date', 'hour'])
    # Todo: notify that job is done
    logger.info("Silver stage complete.")
