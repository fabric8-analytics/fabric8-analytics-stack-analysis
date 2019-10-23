"""Definition of all REST API endpoints."""

import logging
import sys
import os
import hashlib
import json

import flask
import datetime
from flask import Flask, request, g
from flask_cors import CORS
from uuid import uuid1

from analytics_platform.kronos.deployment.submit_training_job import submit_job
import analytics_platform.kronos.gnosis.src.gnosis_constants as gnosis_constants
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME, KRONOS_MODEL_PATH, KRONOS_SCORING_REGION, USE_FILTERS)
from analytics_platform.kronos.src.kronos_online_scoring import (
    load_user_eco_to_kronos_model_dict_s3, score_eco_user_package_dict,
    load_package_frequency_dict_s3)
from analytics_platform.kronos.src.recommendation_validator import RecommendationValidator
from tagging_platform.helles.deployment.submit_npm_tagging_job import submit_tagging_job
from tagging_platform.helles.npm_tagger.get_descriptions_from_s3 import run as \
    run_description_collection
from tagging_platform.helles.npm_tagger.get_version_info_for_missing_packages import run_job as \
    run_missing_package_version_collection_job
from evaluation_platform.uranus.deployment.submit_evaluation_job import (
    submit_evaluation_job)
from util.analytics_platform_util import convert_string2bool_env
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
from raven.contrib.flask import Sentry


def setup_logging(flask_app):
    """Perform the setup of logging (file, log level) for this module."""
    if not flask_app.debug:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'))
        log_level = os.environ.get('FLASK_LOGGING_LEVEL', logging.getLevelName(logging.WARNING))
        handler.setLevel(log_level)
        flask_app.logger.addHandler(handler)


app = Flask(__name__)
setup_logging(app)
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
sentry = Sentry(app, dsn=SENTRY_DSN, logging=True, level=logging.ERROR)
app.logger.info('App initialized, ready to roll...')


CORS(app)

global user_eco_kronos_dict
global eco_to_kronos_dependency_dict
global scoring_status
global all_package_list_obj
global use_filters

hash_dict = dict()

if KRONOS_SCORING_REGION != "":
    app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(
        bucket_name=AWS_BUCKET_NAME,
        additional_path=KRONOS_MODEL_PATH)

    app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
        bucket_name=AWS_BUCKET_NAME,
        additional_path=KRONOS_MODEL_PATH)

    app.all_package_list_obj = RecommendationValidator.load_package_list_s3(
        AWS_BUCKET_NAME, KRONOS_MODEL_PATH)

    app.package_frequency_dict = load_package_frequency_dict_s3(bucket_name=AWS_BUCKET_NAME,
                                                                additional_path=KRONOS_MODEL_PATH)
    app.use_filters = convert_string2bool_env(USE_FILTERS)
    app.logger.info("The total manifest file for this ecosystem are: %d" %
                    app.all_package_list_obj.get_all_list_package_length())
    app.scoring_status = True
else:
    app.scoring_status = False


@app.before_request
def before_request():
    """Remember timestamp where the request handling starts."""
    g.request_start = datetime.datetime.utcnow()


@app.teardown_request
def teardown_request(_):
    """Remember timestamp where the request handling finishes and compute duration."""
    app.logger.debug('Response time: {t} seconds'.format(
        t=(datetime.datetime.utcnow() - g.request_start).total_seconds()))


@app.route('/')
def heart_beat():
    """Handle the / REST API call."""
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/kronos_training', methods=['POST'])
def train_and_save_kronos():
    """Handle the /api/v1/schemas/kronos_training REST API call."""
    app.logger.info("Submitting the training job")

    input_json = request.get_json()
    training_data_url = input_json.get("training_data_url")
    fp_min_support_count = input_json.get(gnosis_constants.FP_MIN_SUPPORT_COUNT_NAME,
                                          gnosis_constants.FP_MIN_SUPPORT_COUNT_VALUE)
    fp_intent_topic_count_threshold = input_json.get(
        gnosis_constants.FP_INTENT_TOPIC_COUNT_THRESHOLD_NAME,
        gnosis_constants.FP_INTENT_TOPIC_COUNT_THRESHOLD_VALUE)
    fp_num_partition = input_json.get(gnosis_constants.FP_NUM_PARTITION_NAME,
                                      gnosis_constants.FP_NUM_PARTITION_VALUE)

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip',
                          training_data_url=training_data_url,
                          fp_min_support_count=str(fp_min_support_count),
                          fp_intent_topic_count_threshold=str(fp_intent_topic_count_threshold),
                          fp_num_partition=str(fp_num_partition))

    return flask.jsonify(response)


@app.route('/api/v1/schemas/kronos_scoring', methods=['POST'])
def predict_and_score():
    """Handle the /api/v1/schemas/kronos_scoring REST API call."""
    input_json = request.get_json()

    # Get the response if already cached earlier
    # Sort package_list elements first
    for i in input_json:
        i['package_list'] = sorted(i['package_list'])

    hash_key = hashlib.sha224(json.dumps(input_json, sort_keys=True).encode('utf-8')).hexdigest()
    if hash_key not in hash_dict:

        app.logger.info("Analyzing the given EPV")
        app.logger.info(input_json)

        response = {"message": "Failed to load model, Kronos Region not available"}

        if app.scoring_status:
            response = score_eco_user_package_dict(
                user_request=input_json,
                user_eco_kronos_dict=app.user_eco_kronos_dict,
                eco_to_kronos_dependency_dict=app.eco_to_kronos_dependency_dict,
                all_package_list_obj=app.all_package_list_obj,
                package_frequency_dict=app.package_frequency_dict,
                use_filters=app.use_filters)

        app.logger.info("Sending back Kronos Response")
        app.logger.info(response)
        hash_dict[hash_key] = response
    else:
        app.logger.info("Sending back Cached Response")
        response = hash_dict[hash_key]
        app.logger.info(response)
    return flask.jsonify(response)


@app.route('/api/v1/schemas/kronos_evaluation', methods=['POST'])
def submit_kronos_evaluation():
    """Handle the /api/v1/schemas/kronos_evaluation REST API call."""
    app.logger.info("Submitting the evaluation job")
    response = {
        "status_description": "Failed to load model, Kronos Region not available"}

    if not app.scoring_status:
        return flask.jsonify(response)

    result_id = str(uuid1())
    input_json = request.get_json()
    training_data_url = input_json.get("training_data_url")
    response = submit_evaluation_job(input_bootstrap_file='/uranus_bootstrap_action.sh',
                                     input_src_code_file='/tmp/testing.zip',
                                     training_url=training_data_url,
                                     result_id=result_id)
    response["evaluation_S3_result_id"] = result_id
    return flask.jsonify(response)


@app.route('/api/v2/npm_tagging', methods=['POST'])
def tag_npm_packages_textrank():
    """Handle the /api/v2/npm_tagging REST API call."""
    input_json = request.get_json()
    response = submit_tagging_job(input_bootstrap_file='/helles_bootstrap_action.sh',
                                  input_src_code_file='/tmp/tagging.zip',
                                  package_name=input_json.get('package_name', ''),
                                  manifest_path=input_json.get('manifest_path', ''))
    return flask.jsonify(response)


@app.route('/api/v2/npm_descriptions', methods=['POST'])
def collect_npm_descriptions():
    """Handle the /api/v2/npm_descriptions REST API call."""
    input_json = request.get_json()
    run_description_collection(input_data_path=input_json.get('input_data_path'))
    return flask.jsonify({"status": "Job completed"})


@app.route('/api/v2/npm_missing_versions', methods=['POST'])
def collect_missing_package_versions():
    """Handle the /api/v2/npm_missing_versions REST API call."""
    input_json = request.get_json()
    run_missing_package_version_collection_job(input_json.get('input_data_path'))
    return flask.jsonify({"status": "Job run successfully"})


if __name__ == "__main__":
    app.run()
