package com.cedexis.lagmonitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import kafka.cluster.Broker;
import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created on 11/23/16.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static int TIMER_MSEC = 10 * 1000;

    public static void main(String[] args) throws Exception {
        Main main = new Main();

        main.startProcess();
    }


    static class StatusHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            byte [] response = "OK".getBytes();
            t.sendResponseHeaders(200, response.length);
            OutputStream os = t.getResponseBody();
            os.write(response);
            os.close();
        }
    }

    public Main() {
    }

    private void startProcess() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File jsonFile = new File(classLoader.getResource("config.json").getFile());

        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<HashMap<String,Object>>() {};

        HashMap<String,Object> configMap = mapper.readValue(jsonFile, typeRef);

        Integer timerMSec = (Integer)configMap.get("timer_msec");

        List<Map<String, String>> topics = (List<Map<String, String>>)configMap.get("topics");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> {
             for(Map<String, String> topicMap : topics) {
                 topicMap.forEach((topic, group) -> {
                     LOGGER.debug(topic + ":" + group);

                     try {

                         if (!getOffsets(topic, group, configMap)) {
                             LOGGER.warn("problem with {}/{} - removing from list", topic, group);
                         //    workList.remove(workItem);
                         }
                         getOffsets(topic, group, configMap);
                     } catch (Exception exception) {
                         LOGGER.error("runLoop exception: topic: {}  group: {}  {}", topic, group, exception);
                     }
                 });

             }

        }, timerMSec, timerMSec, TimeUnit.MILLISECONDS);
    }

/*
    public static void getBrokerList(String zookeeperAddress, String topic) throws IOException,
            KeeperException, InterruptedException {

        ZooKeeper zk = new ZooKeeper(zookeeperAddress, 10000, null);
        List<String> brokerList = new ArrayList<String>();

        List<String> ids = zk.getChildren("/brokers/ids", false);
        for (String id : ids) {
            String brokerInfoString = new String(zk.getData("/brokers/ids/" + id, false, null));
            Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfoString);
            if (broker != null) {
                brokerList.add(broker.connectionString());
            }
        }

        props.put("serializer.class", KAFKA_STRING_ENCODER);
        props.put("metadata.broker.list", String.join(",", brokerList));
        producer = new Producer<String, String>(new ConsoleProducer.ProducerConfig(props));
    }
*/

    private boolean getOffsets(String topic, String group, Map<String, Object> configMap) {
        KafkaConsumer<String, String> kafkaConsumer = getConsumer(group, configMap);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

        // does the topic exist?
        if(partitionInfos == null) {
            LOGGER.warn("t:{} / g:{} not found", topic, group);
            kafkaConsumer.wakeup();
            kafkaConsumer.close();
            return false;
        }

        List<org.apache.kafka.common.TopicPartition>topicAndPartitions = new ArrayList<>();

        for(int i = 0; i < partitionInfos.size(); i++) {
            org.apache.kafka.common.TopicPartition topicAndPartition = new org.apache.kafka.common.TopicPartition(topic, i);
            topicAndPartitions.add(topicAndPartition);
        }

        List<Long>startList = new ArrayList<>();
        List<Long>endList = new ArrayList<>();

        kafkaConsumer.assign(topicAndPartitions);

        for(int i = 0; i < partitionInfos.size(); i++) {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(topicAndPartitions.get(i));
            if(offsetAndMetadata != null) {
                startList.add(offsetAndMetadata.offset());
            }
        }

        // did we find any active partitions?
        if(startList.size() == 0) {
            LOGGER.warn("topic:group not found: {}:{}", topic, group);
            kafkaConsumer.wakeup();
            kafkaConsumer.close();

            return false;
        }

        kafkaConsumer.seekToEnd(topicAndPartitions);

        for(int i = 0; i < partitionInfos.size(); i++) {
            endList.add(i, kafkaConsumer.position(topicAndPartitions.get(i)));
        }

        LOGGER.debug("startlist.size: {}  endlist.size: {}  partitions: {}", startList.size(), endList.size(), partitionInfos.size());

        long sumLag = 0;
        try {
            for (int i = 0; i < partitionInfos.size(); i++) {
                long lStart = startList.get(i);
                long lEnd = endList.get(i);

                sumLag += (lEnd - lStart);
             //   DDog.getDDog().gauge("lag", lEnd - lStart, "partition:" + i, "topic:" + topic, "group:" + group);
                LOGGER.debug("partition: {}  start: {}   end: {}  lag: {}", i, lStart, lEnd, (lEnd - lStart));
            }
        } catch(Exception exception) {
            LOGGER.error("partition count error", exception);
        }

     //   DDog.getDDog().gauge("lag", sumLag, "partition:sum", "topic:" + topic, "group:" + group);

        kafkaConsumer.poll(0);

        topicAndPartitions.clear();
        kafkaConsumer.assign(topicAndPartitions);

        kafkaConsumer.wakeup();
        kafkaConsumer.close();

        return true;
    }

    public String getBrokerList(Map<String, Object> configMap) {
        String brokerList = "";
        List<String> brokers = (List)configMap.get("brokers");
        for(String broker: brokers) {
            brokerList += broker;
            brokerList += ",";
        }

        return brokerList.substring(0, brokerList.length() - 1);
    }


    public KafkaConsumer<String, String> getConsumer(String group, Map<String, Object> configMap) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  getBrokerList(configMap));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put("enable.auto.commit", "false");

        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer kafkaConsumer = new KafkaConsumer<String, String>(config);
        return kafkaConsumer;
    }
}
