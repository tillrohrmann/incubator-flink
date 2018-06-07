FROM flink:native-kubernetes

ENV JOB_TARGET_DIR=/opt/flink/lib
ARG job_jar=NOT_SET

USER root
ADD $job_jar "$JOB_TARGET_DIR"
RUN chown flink -R $JOB_TARGET_DIR
USER flink

