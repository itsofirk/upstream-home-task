import logging
from flask import Flask, jsonify

from upstream import etl
from upstream.common.config import AppConfig, DatalakeConfig
from upstream.infrastructure import set_up_local_data_lake

logger = logging.getLogger(AppConfig.app_name)

app = Flask(AppConfig.app_name)


@app.route('/process', methods=['GET'])
def run_process():
    logger.info("New process request.")
    etl.start_concurrently()
    return jsonify(response="Acknowledged!", status=200, mimetype='application/json')


if __name__ == '__main__':
    logger.info("Setting up application...")
    set_up_local_data_lake(DatalakeConfig.root_path)
    logger.info("Starting application...")
    app.run(host='0.0.0.0', port=5000)
    logger.info("Application started.")
