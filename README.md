# kafka_lagmonitor

App to monitor consumer lag and post to DataDog.

This process looks at the difference between the write and read addresses for a given partition / consumer group combination. 

All settings: https://github.com/ethrbunny/kafka_lagmonitor/blob/master/src/main/resources/config.yaml

- **To build:** ./gradlew clean build  
- **To make a deployable jar:** ./gradlew fatjar (can be combined with above)  
- **To build container:** docker build -t eu.gcr.io/kafka-streaming-log-processing/kafka_lagmonitor:&lt;version> .  
- **To copy to google:**  gcloud docker -- push eu.gcr.io/kafka-streaming-log-processing/kafka_lagmonitor:&lt;version>  
- **To deploy to kubernetes:** kubectl run kafka-lagmonitor-pod --image=eu.gcr.io/kafka-streaming-log-processing/kafka_lagmonitor:&lt;verson>
- **To update a running pod with a new version:** kubectl set image deployment/kafka-lagmonitor-pod kafka-lagmonitor-pod=eu.gcr.io/kafka-streaming-log-processing/kafka_lagmonitor:&lt;version>
