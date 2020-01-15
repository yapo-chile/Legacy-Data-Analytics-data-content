#!/bin/bash

MODULE_COMPILE=""

function GET_BUILD_MODULE(){
    GIT_CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    GIT_LAST_COMMIT=$(git log -p --name-only --oneline | head -1 | awk '{print $1}')
    GIT_LAST_MERGE=$(git log -p --name-only --oneline | grep "Merge" | head -1 | awk '{print $1}')
    echo "GIT_CURRENT_BRANCH: ${GIT_CURRENT_BRANCH}"
    echo "GIT_LAST_COMMIT: ${GIT_LAST_COMMIT}"
    echo "GIT_LAST_MERGE: ${GIT_LAST_MERGE}"
    MODULE_COMPILE=$(git log -p --name-only --oneline ${GIT_LAST_MERGE}..${GIT_LAST_COMMIT} | grep "/" | grep  -v " " | grep -v ".md" | awk '{split($0, val, "/"); print val[1]}' | sort | uniq -c | awk '{print $2}')
}

function BUILD_MODULE(){
    IFS=' '
    read -ra ADDR <<< "${MODULE_COMPILE}"
    for MODULE in "${ADDR[@]}"; do
        echo "make -C ${MODULE} docker-build"
        make -C ${MODULE} docker-build
        echo "make -C ${MODULE} check-style"
        make -C ${MODULE} check-style
    done
}

GET_BUILD_MODULE
BUILD_MODULE

