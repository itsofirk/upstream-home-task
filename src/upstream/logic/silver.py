"""
silver stage logic
"""
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq


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


def standardize_gear_position(df: pd.DataFrame) -> pd.DataFrame:
    ...


def export_parquet(df: pd.DataFrame, path: str) -> None:
    ...


def silver(bronze_dir: str, silver_dir: str) -> None:
    ...
