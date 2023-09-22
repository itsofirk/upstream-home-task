"""
bronze stage logic
"""
import logging

import pandas as pd
import requests
import pyarrow as pa
from pyarrow import parquet as pq

from upstream import datalake
from upstream.common.exceptions import ApiError, DataLakeError

logger = logging.getLogger(__name__)


def get_messages(url, amount) -> list[dict]:
    """
    get_messages fetches a list of messages from the API.

    :param url: Specify the url of the api endpoint
    :param amount: Specify how many messages to fetch
    :return: A list of messages
    """

    logger.info(f"Fetching {amount:,} messages...")
    resp = requests.get(url, params={'amount': amount})

    if resp.status_code != requests.codes.ok:
        logger.error("Failed to fetch messages.")
        raise ApiError(resp.text)

    try:
        return resp.json()
    except requests.exceptions.JSONDecodeError as e:
        logger.error("Failed to parse API response.")
        raise ApiError(e.args[0])


def parse_messages(messages: list[dict], timestamp_unit='ms') -> pd.DataFrame:
    """
    parse_messages takes a list of messages and returns a pandas DataFrame.

    :param messages: list[dict]: Pass the list of messages
    :param timestamp_unit: Specify the unit of time that is used in the timestamp
    :return: A messages DataFrame
    """
    df = pd.DataFrame(messages)
    df['timestamp'] = pd.to_datetime(df.timestamp, unit=timestamp_unit)
    df['date'] = df.timestamp.dt.date
    df['hour'] = df.timestamp.dt.hour
    return df


def export_parquet(df, bronze_dir):
    """
    export_parquet takes a dataframe and writes it to the bronze directory as parquet files partitioned by date and
    hour extracted from the timestamp column.

    :param df: The dataframe to be exported
    :param bronze_dir: Specify the directory where the parquet file will be stored
    :return: The directory path to export the parquet files onto.
    """
    logger.info("Exporting table to bronze_dir...")
    table = pa.Table.from_pandas(df)
    if not datalake.is_empty(bronze_dir):
        raise DataLakeError("Provided directory is not empty.")
    pq.write_to_dataset(table, root_path=bronze_dir, partition_cols=['date', 'hour'])


def bronze(upstream_url, bronze_dir, messages_count=10_000):
    """
    The bronze function takes an upstream URL, a bronze directory, and a messages count.
    It gets messages from the upstream URL, parses them into a dataframe, and exports that dataframe
    to parquets in the bronze directory.

    :param upstream_url: Specify the url of the upstream api
    :param bronze_dir: Specify the directory where the parquet file will be stored
    :param messages_count: The number of messages that are retrieved from the upstream api
    """
    logger.info("Starting the bronze stage...")
    logger.debug("Getting messages...")
    messages = get_messages(upstream_url, messages_count)
    logger.debug("Parsing messages...")
    df = parse_messages(messages)
    logger.debug("Exporting data to bronze_dir...")
    export_parquet(df, bronze_dir)
    # Todo: notify that job is done
    logger.info("Bronze stage complete.")
