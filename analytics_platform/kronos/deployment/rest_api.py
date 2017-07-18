import logging
import sys

import flask
from flask import Flask, request, current_app
from flask_cors import CORS

from analytics_platform.kronos.deployment.submit_training_job import submit_job
from analytics_platform.kronos.src.online_scoring import *
from analytics_platform.kronos.src.offline_training import load_eco_to_kronos_dependency_dict_s3

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
app = Flask(__name__)
CORS(app)

@app.route('/api/v1/schemas/kronos_load', methods=['POST'])
def load_model():
    input_json = request.get_json()
    kronos_data_url = input_json.get("kronos_data_url")

    bucket_name = trunc_string_at(kronos_data_url, "/", 2, 3)
    additional_path = trunc_string_at(kronos_data_url, "/", 3, -1)

    current_app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(bucket_name=bucket_name,
                                                                             additional_path=additional_path)

    current_app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(bucket_name=bucket_name,
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

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip', training_data_url=training_data_url)
    return flask.jsonify(response)


@app.route('/api/v1/schemas/kronos_scoring', methods=['POST'])
def predict_and_score():
    input_json = request.get_json()
    app.logger.info("Analyzing the given EPV")

    response = score_eco_user_package_dict(user_request=input_json,
                                           user_eco_kronos_dict=current_app.user_eco_kronos_dict,
                                           eco_to_kronos_dependency_dict=current_app.eco_to_kronos_dependency_dict)

    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
