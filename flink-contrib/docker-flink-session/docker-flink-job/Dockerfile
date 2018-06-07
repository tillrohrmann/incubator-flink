FROM flink:native-kubernetes

ENV JOB_TARGET_DIR=/opt/jobs
ENV JOB_NAME=job.jar
ENV JOB_SOURCE=throughput.jar

USER root
RUN mkdir -p $JOB_TARGET_DIR
COPY $JOB_SOURCE "$JOB_TARGET_DIR/$JOB_NAME"
RUN chown flink -R $JOB_TARGET_DIR
USER flink

