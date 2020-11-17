#!/usr/bin/env bash

echo "Publishing docker image to Artifactory"
docker push "${DOCKER_IMAGE}"
