import logging
import sys

import flask
from flask import Flask
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


@app.route('/api/v1/softnet_training', methods=['POST'])
def train_and_save_softnet():
    app.logger.info("Submitting the training job")

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/tmp/training.zip')
    return flask.jsonify(response)


if __name__ == "__main__":
    app.run()
