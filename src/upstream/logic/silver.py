"""
silver stage logic
"""
import logging
import numpy as np
import pandas as pd
from upstream import datalake

logger = logging.getLogger(__name__)
GEAR_POSITION_MAPPING = {'NEUTRAL': 0, 'REVERSE': -1, None: np.nan,
                         '-1': -1, '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6}


def load_bronze(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = filter_null_vins(df)
    df = remove_rows_with_bad_manufacturer(df)
    return df


def filter_null_vins(df):
    return df.dropna(subset='vin')


def remove_rows_with_bad_manufacturer(df):
    return df[df.manufacturer.str.strip() == df.manufacturer]


def standardize_gear_position(df: pd.DataFrame, gear_mapping) -> pd.DataFrame:
    """
    Takes a dataframe as input and returns the same dataframe with the gearPosition column standardized converted to
    integers.
    If a value is not found in the gear_mapping, it will be mapped to NaN.
    """
    df['gearPosition'] = df.gearPosition.map(gear_mapping).astype('Int64')
    return df


def silver(bronze_dir: str, silver_dir: str) -> None:
    logger.info("Starting the bronze stage...")
    logger.debug("Loading bronze data...")
    bronze = load_bronze(bronze_dir)
    logger.debug("Cleaning and Filtering bronze data...")
    df = clean_data(bronze)
    logger.debug("Standardizing gear position...")
    df = standardize_gear_position(df, GEAR_POSITION_MAPPING)
    logger.debug("Exporting data to silver_dir...")
    datalake.export_parquet(df, silver_dir, partition_cols=['date', 'hour'])
    # Todo: notify that job is done
    logger.info("Silver stage complete.")
