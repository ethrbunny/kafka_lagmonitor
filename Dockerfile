FROM docker.io/ethrbunny/centos-java:latest
COPY build/libs/kafka_lagmonitor-all.jar /
COPY elastic-apm-agent-1.1.0.jar /

ENV JAVA_OPTS="-javaagent:elastic-apm-agent-1.1.0.jar -Delastic.apm.service_name=kafka_lagmonitor  -Delastic.apm.application_packages=com.cedexis.lagmonitor -Delastic.apm.server_url=http://127.0.0.1:8200"


CMD java -Dlogback.configurationFile=./logback.xml -jar /kafka_lagmonitor-all.jar $JAVA_OPTS
