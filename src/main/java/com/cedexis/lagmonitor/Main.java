package com.cedexis.lagmonitor;

import co.elastic.apm.api.CaptureTransaction;
import com.datastax.driver.core.ResultSet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;



/**
 * Created on 11/23/16.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String TABLE = "topics";
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
                         //    localTopics.remove(topicMap);
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

    @CaptureTransaction
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

    @CaptureTransaction
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

        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<Object> task = () -> {
             for(int i = 0; i < partitionInfos.size(); i++) {
                 try {
                     OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(topicAndPartitions.get(i));
                     if (offsetAndMetadata != null) {
                         startList.add(offsetAndMetadata.offset());
                     }
                 } catch(WakeupException we) {
                     kafkaConsumer.close();
                 }
             }
           return null;
        };

        Future<Object> future = executor.submit(task);
        try {
            Object result = future.get(2, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
           // handle the timeout
            LOGGER.warn("timeout..skipping..");
            kafkaConsumer.wakeup();

            return false;
        } catch (InterruptedException e) {
               // handle the interrupts
        } catch (ExecutionException e) {
               // handle other exceptions
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }

        // did we find any active partitions?
        if(startList.size() == 0) {
            LOGGER.warn("topic:group not found: {}:{}", topic, group);
            kafkaConsumer.wakeup();
            kafkaConsumer.close();

            return false;
        }

        executor = Executors.newCachedThreadPool();
        task = () -> {
             kafkaConsumer.seekToEnd(topicAndPartitions);
            try {
             for(int i = 0; i < partitionInfos.size(); i++) {
                 endList.add(i, kafkaConsumer.position(topicAndPartitions.get(i)));
             }
            } catch(WakeupException we) {
                kafkaConsumer.close();
            }
           return null;
        };

        future = executor.submit(task);
        try {
            Object result = future.get(2, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            // handle the timeout
            LOGGER.warn("timeout..skipping..");
            kafkaConsumer.wakeup();

            return false;
        } catch (InterruptedException e) {
            // handle the interrupts
        } catch (ExecutionException e) {
            // handle other exceptions
        } finally {
            future.cancel(true);
            executor.shutdownNow();
         }

        LOGGER.debug("startlist.size: {}  endlist.size: {}  partitions: {}", startList.size(), endList.size(), partitionInfos.size());

        if(startList.size() != endList.size())  {
            LOGGER.error("startlist.Size() '{}' != endlist.Size() '{}'", startList.size(), endList.size()) ;

            kafkaConsumer.wakeup();
            kafkaConsumer.close();

            return false;
        }

        long sumLag = 0;
        try {
            for (int i = 0; i < partitionInfos.size(); i++) {
                long lStart = startList.get(i);
                long lEnd = endList.get(i);

                sumLag += (lEnd - lStart);
                LOGGER.info("type:partition_lag topic:{} group:{} partition:{} start:{} end:{} lag:{}", topic, group, partitionInfos.get(i).partition(), lStart, lEnd, lEnd - lStart);
            }
        } catch(Exception exception) {
            LOGGER.error("partition count error", exception);
            return false;
        }

        LOGGER.info("type:consumer_lag topic:{} group:{} sum:{}", topic, group, sumLag);

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
