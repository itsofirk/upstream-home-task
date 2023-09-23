import logging
import time

from watchdog.events import FileSystemEventHandler

from upstream.infrastructure import db

logger = logging.getLogger(__name__)


class ParquetFileHandler(FileSystemEventHandler):
    def __init__(self, previous_stage):
        self.stage = previous_stage

    def on_created(self, event):
        if event.is_directory:
            return

        # Assuming Parquet files have a .parquet extension
        if event.src_path.endswith(".parquet"):
            # Record the filename and timestamp in your database
            file_name = event.src_path.split("/")[-1]
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            db.add_file(self.stage, event.src_path, timestamp)
            logger.debug(f"New Parquet file created: {file_name}, timestamp: {timestamp}")
