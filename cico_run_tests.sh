#!/bin/bash

set -ex

. cico_setup.sh

build_image
CI=1 ./qa/runtest.sh
push_image
