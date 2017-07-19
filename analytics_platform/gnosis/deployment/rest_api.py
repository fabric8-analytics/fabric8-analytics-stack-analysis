import logging
import sys

import flask
from flask import Flask, request
from flask_cors import CORS

from analytics_platform.gnosis.deployment.submit_training_job import submit_job
from analytics_platform.gnosis.src.gnosis_constants import *

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
CORS(app)


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/gnosis_training', methods=['POST'])
def train_and_save():
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


if __name__ == "__main__":
    app.run()
