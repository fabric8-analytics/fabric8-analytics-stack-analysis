#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

zip -r /tmp/training.zip /analytics_platform /util

gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT -t $SERVICE_TIMEOUT rest_api:app

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
