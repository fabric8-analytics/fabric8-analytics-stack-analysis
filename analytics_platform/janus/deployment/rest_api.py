import logging
import os
import pickle
import flask
import json
from flask import Flask, request
from flask_cors import CORS
import sys
sys.path.insert(1, '/usr/local/spark/python')
sys.path.insert(2, '/usr/local/spark/python/lib/py4j-0.10.4-src.zip')

from analytics_platform.janus.deployment.submit_training_job import submit_job
from analytics_platform.janus.src import config
from analytics_platform.janus.src.user_category import UserCategorizationModel
from util.data_store.spark_s3_data_store import SparkS3dataStore
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext


# Python2.x: Make default encoding as UTF-8
if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('UTF8')


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
app = Flask(__name__)
app.config['DEBUG']=False
app.config.from_object('analytics_platform.janus.src.config')
CORS(app)


user_model = None
sc = None
sqlContext = None


@app.before_first_request
def load_model():
    global sc, sqlContext, user_model 
    sc = SparkContext()    
    sqlContext = SQLContext(sc)    
    method = config.METHOD
    file_name_prefix = "clustering_results/janus_" + method
    pipeline_store = SparkS3dataStore(src_bucket_name=config.AWS_TARGET_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY, sc=sc, sqlContext=sqlContext)
    assert pipeline_store is not None

    app.user_model = UserCategorizationModel.load(pipeline_store, file_name_prefix)

    assert app.user_model is not None

@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})

@app.route('/api/v1/janus', methods=['POST'])
def find_user_category():
    method = config.METHOD
    response = {}
    if request.method == "POST":
        request_json = request.get_json()
        input_json = ('', json.dumps(request_json))
        app.logger.info("Scoring the given user-information")
        # TODO Validate input JSON schema, use float conversion for numeric data
        load_data = sc.parallelize([input_json])
        response = app.user_model.score(load_data)
    return flask.jsonify(response)

@app.route('/api/v1/janus/training', methods=['POST'])
def submit_training_job():
    app.logger.info("Submitting the training job")

    response = submit_job(input_bootstrap_file='/bootstrap_action.sh',
                          input_src_code_file='/training.zip')
    return flask.jsonify(response)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
