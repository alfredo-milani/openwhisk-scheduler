# docker image from Docker Hub
FROM openjdk:13-jdk-alpine3.10

USER root

RUN mkdir -p /home/openwhisk/bin
RUN mkdir -p /home/openwhisk/conf
RUN mkdir -p /home/openwhisk/script

COPY bin/openwhisk-scheduler-*-jar-with-dependencies.jar /home/openwhisk/bin/ow-scheduler.jar
COPY conf/*.properties /home/openwhisk/conf/conf.properties
COPY script/* /home/openwhisk/script

WORKDIR /home/openwhisk

ENV OW_SCHEDULER_HOME='/home/openwhisk'
ENV PATH="${OW_SCHEDULER_HOME}/bin:${OW_SCHEDULER_HOME}/script:${PATH}"

# for now, use sleep task to maintain container up
# CMD [ "/bin/sh", "-c", "--", "while true ; do sleep 1d; done;" ]
# start jar executable with default configuration
# CMD [ "/bin/sh", "-c", "--", "run.sh" ]
ENTRYPOINT [ "run.sh" ]