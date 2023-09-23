import logging

from upstream.common.config import AppConfig, DatalakeConfig
from upstream.infrastructure import set_up_local_data_lake
from upstream.filewatcher import FileWatcher

logger = logging.getLogger(AppConfig.app_name)


def setup():
    logger.info("Setting up application...")
    set_up_local_data_lake(DatalakeConfig.root_path)
    logger.info("Application set up.")


file_watcher = FileWatcher(DatalakeConfig.root_path, DatalakeConfig.bronze, DatalakeConfig.silver)


if __name__ == '__main__':
    logger.info("Starting application...")
    setup()

    file_watcher.start()
    logger.info("Application started.")
