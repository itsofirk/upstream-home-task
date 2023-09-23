from flask import Flask, jsonify
import logging

from upstream.common.config import AppConfig, DatalakeConfig, UpstreamConfig
from upstream.infrastructure import set_up_local_data_lake
from upstream.filewatcher import FileWatcher
from upstream.logic import bronze, silver, gold

logger = logging.getLogger(AppConfig.app_name)

app = Flask(AppConfig.app_name)
file_watcher = FileWatcher(DatalakeConfig.root_path, DatalakeConfig.bronze, DatalakeConfig.silver)


@app.route('/process')
def run_process():
    logger.info("New process request")
    logger.debug("Running bronze...")
    bronze(UpstreamConfig.url, DatalakeConfig.bronze, UpstreamConfig.amount)
    logger.debug("Running silver...")
    silver(DatalakeConfig.bronze, DatalakeConfig.silver, )
    logger.debug("Running gold...")
    gold(DatalakeConfig.silver, DatalakeConfig.gold)
    return jsonify({"message": "process done"}, status=200, mimetype='application/json')


if __name__ == '__main__':
    logger.info("Setting up application...")
    set_up_local_data_lake(DatalakeConfig.root_path)
    file_watcher.start()
    logger.info("Starting application...")
    app.run(host='0.0.0.0', port=5000, debug=True)
    logger.info("Application started.")
