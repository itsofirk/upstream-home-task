import logging
import time
from datetime import datetime

from upstream.infrastructure import db, sql_queries as SQL
from upstream.common.config import InfraConfig

broker = InfraConfig.get_redis_broker()

logger = logging.getLogger(__name__)


class Service:
    def __init__(self, stage, stage_process, sleep_time=2, *stage_process_args, **stage_process_kwargs):
        self.stage = stage
        self.stage_process = stage_process
        self.stage_process_args = stage_process_args
        self.stage_process_kwargs = stage_process_kwargs
        self.sleep_time = sleep_time

    def main_loop(self):
        logger.info("Starting main loop.")
        while True:
            logger.debug("Waiting for new job ...")
            if self.check_new_job():
                logger.info("New job found.")
                self.run_target()
                logger.info("Job finished.")
            time.sleep(self.sleep_time)

    def check_new_job(self):
        ...

    def run_target(self):
        logger.info("Starting new job...")
        job_id = self.start_new_job(self.stage)
        self.stage_process(*self.stage_process_args, **self.stage_process_kwargs)
        self.end_job(job_id)
        logger.info("Job finished.")

    def start_new_job(self, stage_name) -> int:
        """
        Takes a stage name, creates a new job in the database and returns the id of the newly created job
        The start_time is set to the timestamp when this function was called
        :param stage_name: Create a new job in the database
        :return: The job_id of the new job
        """
        logger.debug("Starting new job...")
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return db.execute(SQL.START_NEW_JOB, stage_name, start_time)[0]

    def end_job(self, job_id):
        """
        Sets the end_time in the row matching the job_id in the jobs table
        The end_time is set to the timestamp when this function was called
        :param self: Represent the instance of the class
        :param job_id: Identify the job that is being ended
        :return: The end time of the job
        """
        logger.debug("Ending job...")
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        db.execute(SQL.END_JOB, end_time, job_id)
