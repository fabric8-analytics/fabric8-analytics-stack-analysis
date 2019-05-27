#!/usr/bin/env bash

# test coverage threshold
COVERAGE_THRESHOLD=50

check_python_version() {
    python3 tools/check_python_version.py 3 6
}

check_python_version

echo "*****************************************"
echo "*** Cyclomatic complexity measurement ***"
echo "*****************************************"
radon cc -s -a -i venv util tagging_platform evaluation_platform

echo "*****************************************"
echo "*** Maintainability Index measurement ***"
echo "*****************************************"
radon mi -s -i venv util tagging_platform evaluation_platform

echo "*****************************************"
echo "*** Unit tests ***"
echo "*****************************************"

python -m pytest -p no:cacheprovider --cov=analytics_platform/ --cov=util/ --cov=tagging_platform/ --cov=evaluation_platform/ --cov-report term-missing --ignore=evaluation_platform/uranus/src/evaluate_data.py --cov-fail-under=$COVERAGE_THRESHOLD -vv /tests/unit_tests/
codecov --token=5dae2a39-7166-4ce9-a6dc-f300c3beadab
