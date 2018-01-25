package eu.imaintenance.toolset.kafka;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

import com.fasterxml.jackson.core.JsonProcessingException;

import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.api.Producer;

public final class ObservationSender<T> implements Producer<T> {
    Logger logger = LoggerFactory.getLogger(Producer.class);
    private final Datastream theStream;
    private final String topic;
    private final String key;
    private final String id;
    private final Properties properties;

    public ObservationSender(String name, Datastream theStream, String topic, String key, List<String> hosts) {
        // create the id based on name and the data-stream-id
        this.theStream = theStream;
        this.id = String.format("%s-%s", name, theStream.getId());
        // 
        this.topic = topic;
        this.key = key;
        
        // create properties
        properties = new Properties();
        properties.put("client.id", this.id);
        properties.put("producer.type", "async");
        properties.put("bootstrap.servers", String.join(",", hosts));
        
        properties.put("acks", "all");

        // If the request fails, the producer can automatically retry,
        properties.put("retries", 0);

        // Specify buffer size in config
        properties.put("batch.size", 16384);

        // Reduce the no of requests less than 0
        properties.put("linger.ms", 1);

        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        properties.put("buffer.memory", 33554432);

        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean send(T value) {
        return send(value, (Callback)null);
    }

    @Override
    public boolean send(T value, Instant when) {
        return send(value, when, (Callback) null);
    }

    @Override
    public boolean send(T value, Instant when, Duration duration) {
        return send(value, when, duration, (Callback)null);
    }

    @Override
    public boolean send(T value, Callback callback) {
        Observation observation = new Observation();
        observation.setDatastream(theStream.withOnlyId());
        observation.setPhenomenonTimeFrom(ZonedDateTime.now());
        observation.setResult(value);
        observation.setValidTime(Interval.of(Instant.now(), Duration.ofSeconds(30)));
        return sendObservation(observation, callback);
    }

 
    @Override
    public boolean send(T value, Instant when, Callback callback) {
        Observation observation = new Observation();
        observation.setDatastream(theStream.withOnlyId());
        observation.setPhenomenonTimeFrom(ZonedDateTime.from(when));
        observation.setResult(value);
        observation.setValidTime(Interval.of(Instant.now(), Duration.ofSeconds(30)));
        return sendObservation(observation, callback);
    }

    @Override
    public boolean send(T value, Instant when, Duration duration, Callback callback) {
        Observation observation = new Observation();
        observation.setDatastream(theStream.withOnlyId());
        observation.setPhenomenonTimeFrom(Interval.of(when, duration));
        observation.setResult(value);
        observation.setValidTime(Interval.of(Instant.now(), Duration.ofSeconds(30)));
        return sendObservation(observation, callback);
    }
    
    private boolean sendObservation(Observation observation, Callback callback) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            String json = ObjectMapperFactory.get().writeValueAsString(observation);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, json);
            if ( callback != null ) {
                producer.send(record, callback);
            }
            else {
                producer.send(record);
            }
            return true;
        } catch (JsonProcessingException e) {
            logger.error(e.getLocalizedMessage());
        } finally {
            producer.close();
        }
        return false;
        
    }


}
