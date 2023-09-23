"""
gold stage logic
"""
import logging
import os.path

import pandas as pd

from upstream.infrastructure import datalake

logger = logging.getLogger(__name__)


def load_silver(silver_dir: str) -> pd.DataFrame:
    return datalake.load_parquet(silver_dir)


def generate_vin_last_state_report(silver_data: pd.DataFrame):
    """
    Generate a report on the last state of each vehicle with the following columns:
    timestamp, front_left_door_state, wipers_state
    """
    vin_last_timestamp_rows = silver_data.groupby('vin')['timestamp'].idxmax()
    return silver_data.loc[vin_last_timestamp_rows, ['vin', 'timestamp', 'frontLeftDoorState', 'wipersState']].reset_index(drop=True)


def generate_top_10_fastest_vehicles_report(silver_data):
    """
    Generate a report on the top 10 fastest vehicles with the following columns:
    date, hour, vin, velocity
    """
    top_velocity_per_hour = silver_data.groupby(['date', 'hour', 'vin'])['velocity'].max().reset_index()

    return top_velocity_per_hour.groupby(['date', 'hour'])\
        .apply(lambda x: x.nlargest(10, 'velocity'))\
        .reset_index(drop=True)


def gold(silver_dir: str, gold_dir: str):
    """
    The gold function takes the silver data, generates the reports, and exports them to the gold directory.
    """
    logger.info("Starting gold stage...")
    logger.debug("Loading silver data...")
    silver_data = load_silver(silver_dir)
    logger.debug("Generating report-1...")
    vin_last_state = generate_vin_last_state_report(silver_data)
    logger.debug("Generating report-2...")
    top_10_fastest_vehicles = generate_top_10_fastest_vehicles_report(silver_data)
    logger.debug("Exporting gold data...")
    vin_last_state_output = os.path.join(gold_dir, 'vin_last_state.parquet')
    datalake.export_parquet(vin_last_state, vin_last_state_output)
    top_10_fastest_vehicles_output = os.path.join(gold_dir, 'top_10_fastest_vehicles')
    datalake.export_parquet(top_10_fastest_vehicles, top_10_fastest_vehicles_output, partition_cols=['date', 'hour'])
    logger.info("Gold stage completed.")


