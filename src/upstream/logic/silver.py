"""
silver stage logic
"""
import numpy as np
import pandas as pd
from upstream import datalake

GEAR_POSITION_MAPPING = {'NEUTRAL': 0, 'REVERSE': -1, None: np.nan,
                         '-1': -1, '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6}


def load_table(path: str) -> pd.DataFrame:
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
    df['gearPosition'] = df.gearPoistion.map(gear_mapping)
    return df


def silver(bronze_dir: str, silver_dir: str) -> None:
    ...
