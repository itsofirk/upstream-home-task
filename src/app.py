import logging
from upstream.common.config import InfraConfig, AppConfig, UpstreamConfig, DatalakeConfig
from upstream.infrastructure.datalake import set_up_local_data_lake
from upstream.infrastructure.db import set_up_app_db
from upstream.filewatcher import FileWatcher

logger = logging.getLogger(AppConfig.app_name)


def setup():
    logger.info("Setting up application...")
    set_up_app_db(InfraConfig.app_data_dir)
    set_up_local_data_lake(DatalakeConfig.root_path)
    logger.info("Application set up.")


file_watcher = FileWatcher(DatalakeConfig.root_path, DatalakeConfig.bronze, DatalakeConfig.silver)


if __name__ == '__main__':
    logger.info("Starting application...")
    setup()

    file_watcher.start()
    logger.info("Application started.")
