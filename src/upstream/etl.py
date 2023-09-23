import logging
import threading

from upstream.common.config import UpstreamConfig, DatalakeConfig
from upstream.logic import bronze, silver, gold

logger = logging.getLogger(__name__)


def process_data():
    logger.info("Starting process...")
    logger.debug("Running bronze...")
    bronze(UpstreamConfig.url, DatalakeConfig.bronze_path(), UpstreamConfig.amount)
    logger.debug("Running silver...")
    silver(DatalakeConfig.bronze_path(), DatalakeConfig.silver_path(), )
    logger.debug("Running gold...")
    gold(DatalakeConfig.silver_path(), DatalakeConfig.gold_path())
    logger.info("Process complete.")


def start_concurrently():
    threading.Thread(target=process_data, daemon=True).start()  # daemon=True will let the processing finish peacefully
