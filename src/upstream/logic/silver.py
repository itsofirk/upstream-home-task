"""
silver stage logic
"""
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq


def load_parquet(path: str) -> pd.DataFrame:
    ...


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    ...


def standardize_gear_position(df: pd.DataFrame) -> pd.DataFrame:
    ...


def export_parquet(df: pd.DataFrame, path: str) -> None:
    ...


def silver(bronze_dir: str, silver_dir: str) -> None:
    ...
