#!/bin/bash

set -ex

. cico_setup.sh

build_image
./runtest.sh
push_image
