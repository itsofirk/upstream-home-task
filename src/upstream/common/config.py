"""
A centralized module for parsing, validating and providing the necessary configurations
"""
import os
from configparser import ConfigParser
from upstream import args

config = ConfigParser()
config.read(args.config)


class AppConfig:
    app_name = config.get('general', 'app_name')


class InfraConfig:
    host = config.get('infrastructure', 'redis_host')
    port = config.getint('infrastructure', 'redis_port')
    db = config.getint('infrastructure', 'redis_db')
    app_data_dir = config.get('infrastructure', 'app_data_dir')

    @classmethod
    def get_redis_broker(cls):
        return f'redis://{cls.host}:{cls.port}/{cls.db}'


class UpstreamConfig:
    url = config.get('upstream', 'url')
    amount = config.getint('upstream', 'amount')


class DatalakeConfig:
    root_path = config.get('datalake', 'data_root')
    bronze_dir = config.get('datalake', 'bronze')
    silver_dir = config.get('datalake', 'silver')
    gold_dir = config.get('datalake', 'gold')

    @classmethod
    def bronze_path(cls):
        return os.path.join(cls.root_path, cls.bronze_dir)

    @classmethod
    def silver_path(cls):
        return os.path.join(cls.root_path, cls.silver_dir)

    @classmethod
    def gold_path(cls):
        return os.path.join(cls.root_path, cls.gold_dir)