FROM openjdk:8
COPY build/libs/kafka_lagmonitor-all.jar /

WORKDIR /
EXPOSE 80

CMD java  -XX:+UseG1GC  -jar kafka_lagmonitor-all.jar

