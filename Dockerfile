FROM docker.io/ethrbunny/centos-java:latest
COPY build/libs/kafka_lagmonitor-all.jar /
CMD java -Dlogback.configurationFile=./logback.xml -jar /kafka_lagmonitor-all.jar 

