#!/bin/bash -xe

# --------------------------------------------------------------------------------------------------
# this script will be executed by aws spark emr during bootstrap process of each node
# we install python dependencies of our training job here
# --------------------------------------------------------------------------------------------------
sudo pip install cython pomegranate uuid boto3 boto pandas sklearn numpy scipy psutil
