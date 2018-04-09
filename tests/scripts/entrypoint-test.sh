#!/usr/bin/env bash

python -m pytest -p no:cacheprovider --cov=analytics_platform/ --cov=util/ --cov=tagging_platform/ --cov=evaluation_platform/ --cov-report term-missing --ignore=evaluation_platform/uranus/src/evaluate_data.py -vv /tests/unit_tests/
