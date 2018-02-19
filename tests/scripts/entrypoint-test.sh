#!/usr/bin/env bash

python -m pytest -p no:cacheprovider --cov=/analytics_platform:/tagging_platform:/evaluation_platform /tests/unit_tests/
