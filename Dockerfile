FROM docker.io/ethrbunny/centos-java:latest
COPY build/libs/kafka_lagmonitor-all.jar /
COPY elastic-apm-agent-1.1.0.jar /

ENV JAVA_OPTS="-javaagent:elastic-apm-agent-1.1.0.jar -Delastic.apm.service_name=kafka_lagmonitor  -Delastic.apm.application_packages=com.cedexis.lagmonitor -Delastic.apm.server_urls=http://10.95.96.144:8200 -Delastic.apm.log_level=DEBUG"


CMD java -Dlogback.configurationFile=./logback.xmli $JAVA_OPTS  -jar /kafka_lagmonitor-all.jar 
