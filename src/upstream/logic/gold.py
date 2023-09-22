"""
gold stage logic
"""
import pandas as pd

from upstream import datalake


def load_silver(silver_dir: str) -> pd.DataFrame:
    return datalake.load_parquet(silver_dir)


def generate_vin_last_state_report(silver_data: pd.DataFrame):
    ...


def generate_top_10_fastest_vehicles_report(silver_data):
    ...


def gold(silver_dir: str, gold_dir: str):
    ...