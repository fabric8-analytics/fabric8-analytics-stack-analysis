import logging
import sys

import flask
from flask import Flask, request
from flask_cors import CORS

from analytics_platform.kronos.deployment.submit_training_job import submit_job
from analytics_platform.kronos.gnosis.src.gnosis_constants import *
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.src.kronos_online_scoring import *
from util.analytics_platform_util import trunc_string_at

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
CORS(app)

global user_eco_kronos_dict
global eco_to_kronos_dependency_dict


@app.route('/api/v1/schemas/kronos_load', methods=['POST'])
def load_model():
    input_json = request.get_json()
    kronos_data_url = input_json.get("kronos_data_url")

    bucket_name = trunc_string_at(kronos_data_url, "/", 2, 3)
    additional_path = trunc_string_at(kronos_data_url, "/", 3, -1)

    app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(bucket_name=bucket_name,
                                                                     additional_path=additional_path)

    app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(bucket_name=bucket_name,
                                                                              additional_path=additional_path)

    app.logger.info("Kronos model got loaded successfully!")

    response = dict()
    response["message"] = "Kronos is loaded successfully"

    return flask.jsonify(response)


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/kronos_training', methods=['POST'])
def train_and_save_kronos():
    app.logger.info("Submitting the training job")

    input_json = request.get_json()
    training_data_url = input_json.get("training_data_url")
    fp_min_support_count = FP_MIN_SUPPORT_COUNT
    fp_intent_topic_count_threshold = FP_INTENT_TOPIC_COUNT_THRESHOLD
    fp_num_partition = FP_NUM_PARTITION
    if "fp_min_support_count" in input_json:
        fp_min_support_count = input_json.get("fp_min_support_count")
    if "fp_intent_topic_count_threshold" in input_json:
        fp_intent_topic_count_threshold = input_json.get("fp_intent_topic_count_threshold")
    if "fp_num_partition" in input_json:
        fp_num_partition = input_json.get("fp_num_partition")

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip', training_data_url=training_data_url,
                          fp_min_support_count=str(fp_min_support_count),
                          fp_intent_topic_count_threshold=str(fp_intent_topic_count_threshold),
                          fp_num_partition=str(fp_num_partition))

    return flask.jsonify(response)


@app.route('/api/v1/schemas/kronos_scoring', methods=['POST'])
def predict_and_score():
    input_json = request.get_json()
    app.logger.info("Analyzing the given EPV")

    response = score_eco_user_package_dict(user_request=input_json,
                                           user_eco_kronos_dict=app.user_eco_kronos_dict,
                                           eco_to_kronos_dependency_dict=app.eco_to_kronos_dependency_dict)

    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
