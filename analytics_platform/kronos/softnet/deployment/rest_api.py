import logging
import sys

import flask
from flask import Flask,request
from flask_cors import CORS

from analytics_platform.softnet.deployment.submit_training_job import submit_job

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
CORS(app)


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/api/v1/schemas/softnet_training', methods=['POST'])
def train_and_save_softnet():
    app.logger.info("Submitting the training job")
    input_json = request.get_json()
    training_data_url = input_json.get("training_data_url")

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip',training_data_url=training_data_url)
    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
