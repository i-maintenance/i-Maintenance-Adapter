package eu.imaintenance.toolset.api;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.producer.Callback;

import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.Thing;

/**
 * Interface provided to the clients for sending their data to the
 * messaging infrastructure. Clients may obtain a {@link Producer}
 * by requesting
 * <pre>
 * ToolsetClient client = new ToolsetClient().withServiceUri("uri");
 * Producer<Double> producer = client.createProducer(
 *          10l,          // id of a datastream to send data to
 *          "topic",      // name of the topic to use for sending
 *          Double.class);// denotes the type of the message-payload
 *                        
 * producer.send(123.3);
 * </pre> 
 * Each call of the {@link Producer#send(Object)} method will create
 * an {@link Observation}, link this observation with the {@link Datastream}
 * and the corresponding {@link Thing}. 
 * @author dglachs
 *
 * @param <T>
 */

public interface Producer<T> {
    /**
     * Send the observed value, the observation time is inserted
     * automatically (actual time)
     * @param value The observed value
     * @return
     */
    boolean send(T value);
    /** 
     * Send the observed value, the observed time must be 
     * provided as {@link Instant}.
     * 
     * @param value The observed value
     * @param when The time, the observation has been made
     * @return
     */
    boolean send(T value, Instant when);
    /**
     * Send the observed value, the observed time range must 
     * be provided with an {@link Instant} and a {@link Duration}.
     * @param value The observed value
     * @param when The start time of the observation
     * @param duration The duration of the observation
     * @return
     */
    boolean send(T value, Instant when, Duration duration);
    /**
     * Send the observed value, the observation time is inserted
     * automatically (actual time)
     * @param value The observed value
     * @param callback The callback routine
     * @return
     */
    boolean send(T value, Callback callback);
    /** 
     * Send the observed value, the observed time must be 
     * provided as {@link Instant}.
     * 
     * @param value The observed value
     * @param when The time, the observation has been made
     * @param callback The callback routine
     * @return
     */
    boolean send(T value, Instant when, Callback callback);
    /**
     * Send the observed value, the observed time range must 
     * be provided with an {@link Instant} and a {@link Duration}.
     * @param value The observed value
     * @param when The start time of the observation
     * @param duration The duration of the observation
     * @param callback The callback routine
     * @return
     */
    boolean send(T value, Instant when, Duration duration, Callback callback);
    /**
     * Close the {@link Producer}. Upon closing the producer is no longer usable!
     */
    void close();
    /**
     * Obtain insights to the {@link Datastream}. 
     * @return
     */
    public Datastream getDatastream();
}
