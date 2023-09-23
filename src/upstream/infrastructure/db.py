import os
import logging
import sqlite3

from upstream.common.config import InfraConfig
from upstream.infrastructure import sql_queries as SQL

logger = logging.getLogger(__name__)
SQLITE_FILENAME = 'upstream.db'
db_path = os.path.join(InfraConfig.app_data_dir, SQLITE_FILENAME)


def set_up_app_db():
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


def add_file(stage, src_path, date_created):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("INSERT INTO file_monitoring (stage, src_path, timestamp) VALUES (?, ?, ?)",
                   (stage, src_path, date_created))
    conn.commit()
    conn.close()
