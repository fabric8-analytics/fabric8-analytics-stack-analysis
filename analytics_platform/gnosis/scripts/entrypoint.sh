#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

zip -r /tmp/training.zip /analytics_platform /util


gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT -t $SERVICE_TIMEOUT rest_api:app
#spark-submit --jars $SPARK_HOME/hadoop-aws-2.7.3.jar analytics_platform/gnosis/deployment/rest_api.py
