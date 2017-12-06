import logging
import sys
import os

import flask
import datetime
from flask import Flask, request, g
from flask_cors import CORS

from analytics_platform.kronos.deployment.submit_training_job import submit_job
import analytics_platform.kronos.gnosis.src.gnosis_constants as gnosis_constants
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME, KRONOS_MODEL_PATH, KRONOS_SCORING_REGION)
from analytics_platform.kronos.src.kronos_online_scoring import \
    load_user_eco_to_kronos_model_dict_s3, score_eco_user_package_dict
from analytics_platform.kronos.src.recommendation_validator import RecommendationValidator
from tagging_platform.helles.deployment.submit_npm_tagging_job import submit_tagging_job
from tagging_platform.helles.npm_tagger.get_descriptions_from_s3 import run as \
    run_description_collection
from tagging_platform.helles.npm_tagger.get_version_info_for_missing_packages import run_job as \
    run_missing_package_version_collection_job
from analytics_platform.kronos.uranus.src.evaluation_requests import (
    submit_evaluation,
    get_evalution_result)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def setup_logging(flask_app):
    if not flask_app.debug:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'))
        log_level = os.environ.get('FLASK_LOGGING_LEVEL', logging.getLevelName(logging.WARNING))
        handler.setLevel(log_level)
        flask_app.logger.addHandler(handler)


app = Flask(__name__)
setup_logging(app)


CORS(app)

global user_eco_kronos_dict
global eco_to_kronos_dependency_dict
global scoring_status
global all_package_list_obj

if KRONOS_SCORING_REGION != "":
    app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(
        bucket_name=AWS_BUCKET_NAME,
        additional_path=KRONOS_MODEL_PATH)

    app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
        bucket_name=AWS_BUCKET_NAME,
        additional_path=KRONOS_MODEL_PATH)

    app.all_package_list_obj = RecommendationValidator.load_package_list_s3(
        AWS_BUCKET_NAME, KRONOS_MODEL_PATH, KRONOS_SCORING_REGION)

    app.scoring_status = True

    app.logger.info("The total manifest file for this ecosystem are: %d" %
                    app.all_package_list_obj.get_all_list_package_length())
else:
    app.scoring_status = False


@app.before_request
def before_request():
    g.request_start = datetime.datetime.utcnow()


@app.teardown_request
def teardown_request(_):
    app.logger.debug('Response time: {t} seconds'.format(
        t=(datetime.datetime.utcnow() - g.request_start).total_seconds()))


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/kronos_training', methods=['POST'])
def train_and_save_kronos():
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
    input_json = request.get_json()

    app.logger.info("Analyzing the given EPV")
    app.logger.info(input_json)

    response = {"message": "Failed to load model, Kronos Region not available"}

    if app.scoring_status:
        response = score_eco_user_package_dict(
            user_request=input_json,
            user_eco_kronos_dict=app.user_eco_kronos_dict,
            eco_to_kronos_dependency_dict=app.eco_to_kronos_dependency_dict,
            all_package_list_obj=app.all_package_list_obj)

    app.logger.info("Sending back Kronos Response")
    app.logger.info(response)

    return flask.jsonify(response)


@app.route('/api/v1/schemas/submit_kronos_evaluation', methods=['POST'])
def submit_kronos_evaluation():
    app.logger.info("Submitting the testing job")
    response = {"message": "Failed to load model, Kronos Region not available"}

    if app.scoring_status:
        input_json = request.get_json()
        training_data_url = input_json.get("training_data_url")
        response = submit_evaluation(training_data_url=training_data_url)
    return flask.jsonify(response)


@app.route('/api/v1/schemas/get_kronos_evaluation/<evaluation_id>', methods=['GET'])
def get_kronos_evaluation(evaluation_id):
    app.logger.info("Submitting the testing job")
    response = {"message": "Failed to load model, Kronos Region not available"}

    if app.scoring_status:
        response = get_evalution_result(evaluation_id=evaluation_id)
    return flask.jsonify(response)


@app.route('/api/v2/npm_tagging', methods=['POST'])
def tag_npm_packages_textrank():
    input_json = request.get_json()
    response = submit_tagging_job(input_bootstrap_file='/helles_bootstrap_action.sh',
                                  input_src_code_file='/tmp/tagging.zip',
                                  package_name=input_json.get('package_name', ''),
                                  manifest_path=input_json.get('manifest_path', ''))
    return flask.jsonify(response)


@app.route('/api/v2/npm_descriptions', methods=['POST'])
def collect_npm_descriptions():
    input_json = request.get_json()
    run_description_collection(input_data_path=input_json.get('input_data_path'))
    return flask.jsonify({"status": "Job completed"})


@app.route('/api/v2/npm_missing_versions', methods=['POST'])
def collect_missing_package_versions():
    input_json = request.get_json()
    run_missing_package_version_collection_job(input_json.get('input_data_path'))
    return flask.jsonify({"status": "Job run successfully"})


if __name__ == "__main__":
    app.run()
