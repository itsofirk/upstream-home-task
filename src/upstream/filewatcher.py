import os
import threading
import atexit
import logging
from watchdog.observers import Observer
from upstream.infrastructure.parquet_file_handler import ParquetFileHandler

logger = logging.getLogger(__name__)


class FileWatcher:
    def __init__(self, bronze_name, bronze_path, silver_name, silver_path):
        silver_event_handler = ParquetFileHandler(bronze_name)
        gold_event_handler = ParquetFileHandler(silver_name)

        self.observer = Observer()
        self.observer.schedule(silver_event_handler, path=bronze_path, recursive=True)
        self.observer.schedule(gold_event_handler, path=silver_path, recursive=True)

        self.thread = threading.Thread(target=self.observer.start, daemon=True)

        atexit.register(self.observer.stop)  # atexit will call observer.stop() when program exits

    def start(self):
        logger.info("Starting file watcher.")
        self.thread.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()
        logger.info("File watcher stopped.")

