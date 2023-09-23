import os
import logging
import sqlite3

from upstream.infrastructure import sql_queries as SQL

logger = logging.getLogger(__name__)
SQLITE_FILENAME = 'upstream.db'


def set_up_app_db(app_data_dir):
    db_path = os.path.join(app_data_dir, SQLITE_FILENAME)
    logger.info("Setting up database...")
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # create file_monitoring table
    logger.debug("Creating file_monitoring table...")
    cur.execute(SQL.CREATE_FILE_MONITORING_TABLE)
    # create stage_execution table
    logger.debug("Creating stage_execution table...")
    cur.execute(SQL.CREATE_STAGE_EXECUTION_TABLE)

    # close connection
    cur.close()
    con.close()
    logger.info("Database set up.")
