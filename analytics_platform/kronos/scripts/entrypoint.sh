#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

zip -r /tmp/training.zip /analytics_platform /util
zip -r /tmp/tagging.zip /tagging_platform /util /analytics_platform

gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT --workers=2 -k gevent -t $SERVICE_TIMEOUT rest_api:app

# --------------------------------------------------------------------------------------------------
# to make the container alive for indefinite time
# --------------------------------------------------------------------------------------------------
#touch /tmp/a.txt
#tail -f /tmp/a.txt

# --------------------------------------------------------------------------------------------------
# run directly
# ------------------------------------------
#export PYTHONPATH=`pwd`



#python ./analytics_platform/kronos/src/kronos_pgm.py
