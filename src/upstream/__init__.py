import os
import logging

# Logger setup
logger = logging.getLogger(__name__)

level = logging.INFO
if os.getenv('UPSTREAM_VERBOSE'):
    level = logging.DEBUG

logger.setLevel(level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

