#!/usr/bin/env sh

# start scheduler using KAFKA_HOSTS environment variable
java -jar "${OW_SCHEDULER_HOME}/bin/ow-scheduler.jar" --log trace --kafka-bootstrap-servers "${KAFKA_HOSTS}" "${@}"