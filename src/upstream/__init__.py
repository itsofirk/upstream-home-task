import argparse
import logging
import os

# Argparse setup
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity')
parser.add_argument('-c', '--config', help='config file')

if os.getenv('UPSTREAM_CONFIG'):
    parser.set_defaults(config=os.getenv('UPSTREAM_CONFIG'))
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
