#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

zip -r /tmp/training.zip /analytics_platform /util
zip -r /tmp/tagging.zip /tagging_platform /util /analytics_platform
zip -r /tmp/testing.zip /evaluation_platform /util /analytics_platform

# WARNING: Don't add worker class as gevent since it doesn't allow PGM to perform scoring in parallel
gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT --workers=$NUMBER_OF_PROCESSES -t $SERVICE_TIMEOUT rest_api:app

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
