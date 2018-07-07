package com.cedexis.lagmonitor;

import com.datastax.driver.core.ResultSet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.time.LocalDateTime.now;


/**
 * Created on 11/23/16.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String TABLE = "consumer_lag.lag";
    private static final String KEYSPACE = "consumer_lag";

    private static int TIMER_MSEC = 10 * 1000;
    private static List<Map<String, String>> topics = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        Main main = new Main();

        main.startProcess();
    }

    public Main() {
    }

    public Map<String, Object> loadConfig() {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream jsonFile = classloader.getResourceAsStream("config.json");

        HashMap<String, Object> configMap;
        try {
            configMap = mapper.readValue(jsonFile, typeRef);
            return configMap;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    public void initCassandra(Map<String, Object> configMap) {
        List<String>cassandraNodes = (List)configMap.get("cassandra");
        List<InetAddress> nodes = new ArrayList<>();
        for(String node : cassandraNodes) {
            try {
                InetAddress address = InetAddress.getByName(node);
                nodes.add(address);
            } catch(Exception exception) {
                throw new RuntimeException(exception);
            }
        }

        CassandraConnector cassandraConnector = new CassandraConnector();
        cassandraConnector.connect(nodes);

    }

    public synchronized void updateTopics(List<Map<String, String>>list ) {
        topics = list;
    }

    public void startProcess() {
        Map<String, Object> configMap = loadConfig();
        initCassandra(configMap);

        // topics
        Integer topicTimerMSec = (Integer)configMap.get("reload_msec");

        ScheduledExecutorService executorTopics = Executors.newScheduledThreadPool(1);
        executorTopics.scheduleAtFixedRate(() -> {
            List<Map<String, String>> newList = new ArrayList<>();

            List<Map<String, String>> topicMap = getTopics();
            for(Map<String, String> topic : topicMap) {
                newList.add(topic);

                Set<Map.Entry<String, String>> entries = topic.entrySet();
                for(Map.Entry<String, String> entry : entries) {
                    LOGGER.debug("loading {}:{}", entry.getKey(), entry.getValue());
                }
            }

            updateTopics(newList);

        }, 0, topicTimerMSec, TimeUnit.MILLISECONDS);

        Integer lagTimerMSec = (Integer)configMap.get("timer_msec");

        // lag
        ScheduledExecutorService executorLag = Executors.newScheduledThreadPool(1);
        executorLag.scheduleAtFixedRate(() -> {
            // local copy we can edit
             List<Map<String, String>> localTopics = topics;
             for(Map<String, String> topicMap : topics) {
                 topicMap.forEach((topic, group) -> {
                     LOGGER.debug("reading lag: {}:{}", topic, group);
                     try {
                         if (!getOffsets(topic, group, configMap)) {
                             LOGGER.warn("problem with {}/{} ", topic, group);
                             localTopics.remove(topicMap);
                         }
                     } catch (Exception exception) {
                         LOGGER.error("runLoop exception: topic: {}  group: {}  {}", topic, group, exception);
                     }
                 });

             }

             updateTopics(localTopics);

        }, lagTimerMSec, lagTimerMSec, TimeUnit.MILLISECONDS);
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

    public List<Map<String, String>>getTopics() {
        List<Map<String, String>> returnMap = new ArrayList<>();
        ResultSet rows = CassandraConnector.getSession().execute("select topic, group from consumer_lag.topics");
        rows.forEach((row)-> {
            Map<String, String> rowMap = new HashMap<>();
            rowMap.put(row.getString("topic"), row.getString("group"));
            returnMap.add(rowMap);
        });

        return returnMap;
    }

    public void insertValue(String topic, String group, int partition, long lag) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE).append("(topic, group, partition, lag, stamp) ")
                .append("VALUES ('").append(topic)
                .append("', '").append(group)
                .append("', ").append(partition)
                .append(", ").append(lag)
                .append(", '").append(now())
                .append("') using ttl 86400;");

        String query = sb.toString();
        CassandraConnector.getSession().execute(query);
    }

    public boolean getOffsets(String topic, String group, Map<String, Object> configMap) {
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

                insertValue(topic, group, partitionInfos.get(i).partition(), (lEnd - lStart));
                LOGGER.debug("topic: {}  group: {} partition: {}  start: {}   end: {}  lag: {}", topic, group, partitionInfos.get(i).partition(), lStart, lEnd, (lEnd - lStart));
            }
        } catch(Exception exception) {
            LOGGER.error("partition count error", exception);
        }

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
