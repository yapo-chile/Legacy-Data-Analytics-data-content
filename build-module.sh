#!/bin/bash

FILE_LOG=automated_build.txt
function GET_BUILD_MODULE(){
    GIT_CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    git log -p --name-only --oneline > ${FILE_LOG}
    GIT_LAST_COMMIT=$(cat ${FILE_LOG} | head -1 | awk '{print $1}')
    GIT_LAST_MERGE=$(cat ${FILE_LOG} | grep "Merge" | head -1 | awk '{print $1}')
    echo "GIT_CURRENT_BRANCH: ${GIT_CURRENT_BRANCH}"
    echo "GIT_LAST_COMMIT: ${GIT_LAST_COMMIT}"
    echo "GIT_LAST_MERGE: ${GIT_LAST_MERGE}"
    git log -p --name-only --oneline $GIT_LAST_MERGE..$GIT_LAST_COMMIT > ${FILE_LOG}
    cat ${FILE_LOG} | grep "/" | grep  -v " " | grep -v ".md" | awk '{split($0, val, "/"); print val[1]}' | sort | uniq -c | awk '{print $2}' > ${FILE_LOG}
}


function BUILD_MODULE(){
    while IFS= read -r MODULE
    do
        echo "make -C ${MODULE} docker-build"
        make -C "${MODULE}" docker-build
        echo "make -C ${MODULE} check-style"
        make -C "${MODULE}" check-style
    done < "${FILE_LOG}"
}

GET_BUILD_MODULE
BUILD_MODULE

