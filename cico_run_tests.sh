#!/bin/bash

set -ex

. cico_setup.sh

build_image

push_image
