import argparse
import logging

# Argparse setup
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity')
parser.add_argument('-c', '--config', required=True, help='config file')
args = parser.parse_args()

# Logger setup
logger = logging.getLogger(__name__)

level = logging.INFO
if args.verbose:
    level = logging.DEBUG

logger.setLevel(level)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

__all__ = [args]
