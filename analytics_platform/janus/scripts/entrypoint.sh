#!/usr/bin/env bash

export USER_ID=$(id -u)
export GROUP_ID=$(id -g)
envsubst < /tmp/passwd.template > /tmp/passwd
export LD_PRELOAD=libnss_wrapper.so
export NSS_WRAPPER_PASSWD=/tmp/passwd
export NSS_WRAPPER_GROUP=/etc/group

# --------------------------------------------------------------------------------------------------
# zip the entire analytics_platform and util src code to create a python 'package'
# this package will be sent into spark cluster for training job
# --------------------------------------------------------------------------------------------------
# zip -r /training.zip /analytics_platform /util

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

python analytics_platform/janus/deployment/test_flask.py &
spark-submit --jars $SPARK_HOME/hadoop-aws-2.7.3.jar analytics_platform/janus/deployment/rest_api.py