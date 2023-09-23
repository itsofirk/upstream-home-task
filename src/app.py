from flask import Flask, jsonify
import logging

from upstream.common.config import AppConfig, DatalakeConfig, UpstreamConfig
from upstream.infrastructure import set_up_local_data_lake
from upstream.filewatcher import FileWatcher
from upstream.logic import bronze, silver, gold

logger = logging.getLogger(AppConfig.app_name)

app = Flask(AppConfig.app_name)
file_watcher = FileWatcher(DatalakeConfig.bronze_name, DatalakeConfig.bronze_path(), DatalakeConfig.silver_name,
                           DatalakeConfig.silver_path())


@app.route('/bronze')
def run_bronze():
    logger.info("Running bronze stage...")
    bronze(UpstreamConfig.url, DatalakeConfig.bronze_path(), UpstreamConfig.amount)
    return jsonify(response="Bronze stage done!", status=200, mimetype='application/json')


@app.route('/silver')
def run_silver():
    logger.info("Running silver stage...")
    silver(DatalakeConfig.bronze_path(), DatalakeConfig.silver_path())
    return jsonify(response="Silver stage done!", status=200, mimetype='application/json')


@app.route('/gold')
def run_gold():
    logger.info("Running gold stage...")
    gold(DatalakeConfig.silver_path(), DatalakeConfig.gold_path())
    return jsonify(response="Gold stage done!", status=200, mimetype='application/json')


@app.route('/process')
def run_process():
    logger.info("New process request")
    logger.debug("Running bronze...")
    bronze(UpstreamConfig.url, DatalakeConfig.bronze_path(), UpstreamConfig.amount)
    logger.debug("Running silver...")
    silver(DatalakeConfig.bronze_path(), DatalakeConfig.silver_path(), )
    logger.debug("Running gold...")
    gold(DatalakeConfig.silver_path(), DatalakeConfig.gold_path())
    return jsonify(response="Done!", status=200, mimetype='application/json')


if __name__ == '__main__':
    logger.info("Setting up application...")
    set_up_local_data_lake(DatalakeConfig.root_path)
    file_watcher.start()
    logger.info("Starting application...")
    app.run(host='0.0.0.0', port=5000)
    logger.info("Application started.")
