import os
import logging
import sqlite3
from datetime import datetime

from upstream.infrastructure import sql_queries as SQL

logger = logging.getLogger(__name__)
SQLITE_FILENAME = 'sqlite3.db'


class Database:
    def __init__(self, app_data_dir, file_name=SQLITE_FILENAME):
        self.db_path = os.path.join(app_data_dir, file_name)
        logger.info("Setting up database...")

        with Database.connection(self.db_path) as con:
            logger.debug("Creating file_monitoring table...")
            con.execute(SQL.CREATE_FILE_MONITORING_TABLE)

            logger.debug("Creating stage_execution table...")
            con.execute(SQL.CREATE_STAGE_EXECUTION_TABLE)

        logger.info("Database set up.")

    @staticmethod
    def connection(db_path):
        return sqlite3.connect(db_path)

    def execute(self, query, *query_params):
        with Database.connection(self.db_path) as con:
            cur = con.execute(query, tuple(query_params))
            return cur.fetchall()
