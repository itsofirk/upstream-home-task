import logging

from watchdog.events import FileSystemEventHandler

from upstream.infrastructure import db, datalake
from upstream.infrastructure.sql_queries import INSERT_NEW_FILE

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
            file_name = datalake.get_filename(event.src_path)
            timestamp = datalake.get_create_time(event.src_path).strftime('%Y-%m-%d %H:%M:%S.%f')
            self.add_file_to_db(self.stage, event.src_path, timestamp)
            logger.debug(f"New Parquet file created: {file_name}, timestamp: {timestamp}")

    def add_file_to_db(self, stage, src_path, date_created):
        logger.debug("Adding file to database...")
        db.execute(INSERT_NEW_FILE, stage, src_path, date_created)
