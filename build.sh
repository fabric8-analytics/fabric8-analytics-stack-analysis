#!/bin/bash

echo 'building image'
docker build -t docker.io/surajd/kronos:latest -f Dockerfile.kronos .

echo 'pushing image'
docker push docker.io/surajd/kronos:latest

echo 'deploy secret'
oc apply -f secret.yaml

