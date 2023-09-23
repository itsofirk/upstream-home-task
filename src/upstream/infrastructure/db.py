import os
import sqlite3

from upstream.common.config import InfraConfig
from upstream.infrastructure import sql_queries as SQL

SQLITE_FILENAME = 'upstream.db'
db_path = os.path.join(InfraConfig.app_data_dir, SQLITE_FILENAME)


def set_up_data_lake_db():
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # create file_monitoring table
    cur.execute(SQL.CREATE_FILE_MONITORING_TABLE)

    # create stage_execution table
    cur.execute(SQL.CREATE_STAGE_EXECUTION_TABLE)

    # close connection
    cur.close()
    con.close()
