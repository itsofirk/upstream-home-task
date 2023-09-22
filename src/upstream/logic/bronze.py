"""
bronze stage logic
"""
import logging
import requests
import pandas
from pyarrow import parquet as pq
from datetime import datetime

from upstream.common.exceptions import ApiError

logger = logging.getLogger(__name__)


def get_messages(url, amount):
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


def parse_messages(messages):
    ...


def export_parquet(df, bronze_dir):
    ...


def bronze(upstream_url, bronze_dir, messages_count=10_000):
    messages = get_messages(upstream_url, messages_count)
    df = parse_messages(messages)
    export_parquet(df, bronze_dir)
