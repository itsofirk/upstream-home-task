import logging

from upstream.common.config import AppConfig, DatalakeConfig, UpstreamConfig
from upstream.infrastructure import set_up_local_data_lake
from upstream.filewatcher import FileWatcher
from upstream.logic import bronze, silver, gold

logger = logging.getLogger(AppConfig.app_name)

file_watcher = FileWatcher(DatalakeConfig.bronze_name, DatalakeConfig.bronze_path(), DatalakeConfig.silver_name,
                           DatalakeConfig.silver_path())


def run_process():
    logger.info("New process request")
    logger.debug("Running bronze...")
    bronze(UpstreamConfig.url, DatalakeConfig.bronze_path(), UpstreamConfig.amount)
    logger.debug("Running silver...")
    silver(DatalakeConfig.bronze_path(), DatalakeConfig.silver_path(), )
    logger.debug("Running gold...")
    gold(DatalakeConfig.silver_path(), DatalakeConfig.gold_path())
    return dict(response="Done!", status=200, mimetype='application/json')


if __name__ == '__main__':
    logger.info("Setting up application...")
    set_up_local_data_lake(DatalakeConfig.root_path)
    file_watcher.start()
    logger.info("Starting application...")
    logger.info("Application started.")
