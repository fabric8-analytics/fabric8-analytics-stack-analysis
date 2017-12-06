#!/bin/bash

set -ex

. cico_setup.sh

build_image
CI=1 ./runtest.sh
push_image
