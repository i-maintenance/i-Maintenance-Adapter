package eu.imaintenance.toolset.api;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.producer.Callback;



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
}
