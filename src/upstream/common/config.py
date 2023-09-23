"""
A centralized module for parsing, validating and providing the necessary configurations
"""
import os
from configparser import ConfigParser

config = ConfigParser()
if not os.getenv('UPSTREAM_CONFIG'):
    raise ValueError('UPSTREAM_CONFIG is not set')
config.read(os.getenv('UPSTREAM_CONFIG'))


class AppConfig:
    app_name = config.get('general', 'app_name')


class UpstreamConfig:
    url = config.get('upstream', 'url')
    amount = config.getint('upstream', 'amount')


class DatalakeConfig:
    root_path = config.get('datalake', 'data_root')
    bronze_name = config.get('datalake', 'bronze')
    silver_name = config.get('datalake', 'silver')
    gold_name = config.get('datalake', 'gold')

    @classmethod
    def bronze_path(cls):
        return os.path.join(cls.root_path, cls.bronze_name)

    @classmethod
    def silver_path(cls):
        return os.path.join(cls.root_path, cls.silver_name)

    @classmethod
    def gold_path(cls):
        return os.path.join(cls.root_path, cls.gold_name)
