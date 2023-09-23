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

    def start_new_job(self, stage_name) -> int:
        """
        Takes a stage name, creates a new job in the database and returns the id of the newly created job
        The start_time is set to the timestamp when this function was called
        :param stage_name: Create a new job in the database
        :return: The job_id of the new job
        """
        logger.debug("Starting new job...")
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return self.execute(SQL.START_NEW_JOB, stage_name, start_time)[0]

    def end_job(self, job_id):
        """
        Sets the end_time in the row matching the job_id in the jobs table
        The end_time is set to the timestamp when this function was called
        :param self: Represent the instance of the class
        :param job_id: Identify the job that is being ended
        :return: The end time of the job
        :doc-author: Trelent
        """
        logger.debug("Ending job...")
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.execute(SQL.END_JOB, end_time, job_id)
