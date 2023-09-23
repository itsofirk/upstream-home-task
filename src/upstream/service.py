import logging
import time

from upstream.infrastructure import db
from upstream.common.config import InfraConfig

broker = InfraConfig.get_redis_broker()

logger = logging.getLogger(__name__)


class Service:
    def __init__(self, stage, stage_process, sleep_time=2, *):
        self.stage = stage
        self.stage_process = stage_process
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
        job_id = db.start_new_job()
        self.stage_process()
        db.end_job(job_id)
        logger.info("Job finished.")
