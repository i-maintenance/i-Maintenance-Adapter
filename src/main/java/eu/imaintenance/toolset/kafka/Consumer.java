package eu.imaintenance.toolset.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import eu.imaintenance.toolset.observation.ObservationProcessor;

public class Consumer implements Runnable {
    private Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final int id;
    private final String host;
    private final List<String> topics;
    private final ObservationProcessor processor;
    

    public Consumer(int id, String groupId, List<String> hosts, List<String> topics, ObservationProcessor processor) {
        this.id = id;
        this.topics = topics;
        this.host = String.join(",", hosts);
        this.processor = processor;
        Properties props = new Properties();
        props.put("bootstrap.servers", this.host);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    logger.trace(data.toString());
                    
                    try {
                        processor.processKafkaMessage(record.topic(), record.key(), record.value());
                    } catch (ServiceFailureException e) {
                        // TODO Auto-generated catch block - add logger
                        e.printStackTrace();
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}