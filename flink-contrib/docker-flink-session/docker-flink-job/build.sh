#!/bin/bash

TMPDIR=_TMP_

cleanup() {
    rm -rf "${TMPDIR}"
}
trap cleanup EXIT

mkdir -p "${TMPDIR}"

JOB_JAR_SOURCE=$1
NAME=$2
JOB_JAR_TARGET=${TMPDIR}/"job.jar"

cp ${JOB_JAR_SOURCE} ${JOB_JAR_TARGET}
docker build --build-arg job_jar=${JOB_JAR_TARGET} -t ${NAME} .
