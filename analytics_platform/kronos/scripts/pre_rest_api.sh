#!/bin/bash -xe

# --------------------------------------------------------------------------------------------------
# start the script which runs Kronos testing periodically 
# --------------------------------------------------------------------------------------------------

echo [] | cat >/tmp/queue.json
echo {} | cat >/tmp/request_dict.json
PYTHONPATH=`/analytics_platform` python /analytics_platform/kronos/uranus/src/testing_kronos.py &