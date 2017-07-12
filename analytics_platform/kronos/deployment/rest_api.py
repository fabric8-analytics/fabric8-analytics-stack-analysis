import logging
import sys

import flask
from flask import Flask, request
from flask_cors import CORS

from analytics_platform.kronos.deployment.submit_training_job import submit_job
from analytics_platform.kronos.src.online_scoring import *
from analytics_platform.kronos.src.offline_training import load_eco_to_kronos_dependency_dict_s3

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
CORS(app)

global user_eco_kronos_dict
global eco_to_kronos_dependency_dict


@app.before_first_request
def load_model():
    app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3()

    app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3()

    app.logger.info("Kronos model got loaded successfully!")


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/kronos_training', methods=['POST'])
def train_and_save_kronos():
    app.logger.info("Submitting the training job")

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip')
    return flask.jsonify(response)


@app.route('/api/v1/kronos_score', methods=['POST'])
def predict_and_score():
    input_json = request.get_json()
    app.logger.info("Analyzing the given EPV")

    response = score_eco_user_package_dict(user_request=input_json, user_eco_kronos_dict=app.user_eco_kronos_dict,
                                           eco_to_kronos_dependency_dict=app.eco_to_kronos_dependency_dict)

    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
