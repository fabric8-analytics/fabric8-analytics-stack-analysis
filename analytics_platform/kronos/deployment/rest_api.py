import logging
import sys

import flask
from flask import Flask, request
from flask_cors import CORS

from analytics_platform.kronos.deployment.submit_training_job import submit_job
from analytics_platform.kronos.gnosis.src.gnosis_constants import *
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.src.config import AWS_BUCKET_NAME, KRONOS_MODEL_PATH, KRONOS_SCORING_REGION
from analytics_platform.kronos.src.kronos_online_scoring import *

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
CORS(app)

global user_eco_kronos_dict
global eco_to_kronos_dependency_dict
global scoring_status

if KRONOS_SCORING_REGION !="":
  app.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(bucket_name=AWS_BUCKET_NAME,
                                                                 additional_path=KRONOS_MODEL_PATH)

  app.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(bucket_name=AWS_BUCKET_NAME,
                                                                          additional_path=KRONOS_MODEL_PATH)
  app.scoring_status = True
else:
  app.scoring_status = False

@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/kronos_training', methods=['POST'])
def train_and_save_kronos():
    app.logger.info("Submitting the training job")

    input_json = request.get_json()
    training_data_url = input_json.get("training_data_url")
    fp_min_support_count = input_json.get(FP_MIN_SUPPORT_COUNT_NAME, FP_MIN_SUPPORT_COUNT_VALUE)
    fp_intent_topic_count_threshold = input_json.get(FP_INTENT_TOPIC_COUNT_THRESHOLD_NAME,
                                                     FP_INTENT_TOPIC_COUNT_THRESHOLD_VALUE)
    fp_num_partition = input_json.get(FP_NUM_PARTITION_NAME, FP_NUM_PARTITION_VALUE)

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip', training_data_url=training_data_url,
                          fp_min_support_count=str(fp_min_support_count),
                          fp_intent_topic_count_threshold=str(fp_intent_topic_count_threshold),
                          fp_num_partition=str(fp_num_partition))

    return flask.jsonify(response)


@app.route('/api/v1/schemas/kronos_scoring', methods=['POST'])
def predict_and_score():
    input_json = request.get_json()
    app.logger.error("Analyzing the given EPV")
    app.logger.error(input_json)
    response = {"message": "Failed to load model, Kronos Region not available"}
    
    if app.scoring_status:
      response = score_eco_user_package_dict(user_request=input_json,
                                           user_eco_kronos_dict=app.user_eco_kronos_dict,
                                           eco_to_kronos_dependency_dict=app.eco_to_kronos_dependency_dict)

    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
