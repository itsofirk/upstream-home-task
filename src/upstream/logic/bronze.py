"""
bronze stage logic
"""

import requests
import pandas
from pyarrow import parquet as pq
from datetime import datetime


def get_messages(url, amount):
    ...


def parse_messages(messages):
    ...


def export_parquet(df, bronze_dir):
    ...


def bronze(upstream_url, bronze_dir, messages_count=10_000):
    messages = get_messages(upstream_url, messages_count)
    df = parse_messages(messages)
    export_parquet(df, bronze_dir)
