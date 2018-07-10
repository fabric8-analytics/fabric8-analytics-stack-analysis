#!/bin/bash -ex

load_jenkins_vars() {
    if [ -e "jenkins-env.json" ]; then
        eval "$(./env-toolkit load -f jenkins-env.json \
                DEVSHIFT_TAG_LEN \
                QUAY_USERNAME \
                QUAY_PASSWORD \
                JENKINS_URL \
                GIT_BRANCH \
                GIT_COMMIT \
                BUILD_NUMBER \
                ghprbSourceBranch \
                ghprbActualCommit \
                BUILD_URL \
                ghprbPullId)"
    fi
}

prep() {
    yum -y update
    yum -y install docker git
    systemctl start docker
}

build_image() {
    local push_registry
    push_registry=$(make get-push-registry)
    # login before build to be able to pull RHEL parent image
    if [ -n "${QUAY_USERNAME}" -a -n "${QUAY_PASSWORD}" ]; then
        docker login -u "${QUAY_USERNAME}" -p "${QUAY_PASSWORD}" "${push_registry}"
    else
        echo "Could not login, missing credentials for the registry"
        exit 1
    fi
    # Also runs the recipe for docker-build
    make docker-build-test
}

tag_push() {
    local target=$1
    local source=$2
    docker tag "${source}" "${target}"
    docker push "${target}"
}

push_image() {
    local image_name
    local image_repository
    local short_commit
    local push_registry
    image_name=$(make get-image-name)
    image_repository=$(make get-image-repository)
    short_commit=$(git rev-parse --short=$DEVSHIFT_TAG_LEN HEAD)
    push_registry=$(make get-push-registry)

    if [ -n "${ghprbPullId}" ]; then
        # PR build
        pr_id="SNAPSHOT-PR-${ghprbPullId}"
        tag_push "${push_registry}/${image_repository}:${pr_id}" "${image_name}"
        tag_push "${push_registry}/${image_repository}:${pr_id}-${short_commit}" "${image_name}"
    else
        # master branch build
        tag_push "${push_registry}/${image_repository}:latest" "${image_name}"
        tag_push "${push_registry}/${image_repository}:${short_commit}" "${image_name}"
    fi

    echo 'CICO: Image pushed, ready to update deployed app'
}

load_jenkins_vars
prep
