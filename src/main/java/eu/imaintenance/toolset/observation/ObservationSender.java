package eu.imaintenance.toolset.observation;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.threeten.extra.Interval;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.api.Producer;
import eu.imaintenance.toolset.kafka.Sender;

public final class ObservationSender<T> implements Producer<T> {
    private final Datastream theStream;

    private Sender sender;

    public ObservationSender(String name, Datastream stream, String topic, String key, List<String> hosts) throws ServiceFailureException {
        // create the id based on name and the data-stream-id
        this.theStream = stream;
        // create the new sender
        sender = new Sender(String.format("%s - %s (%s)", name, theStream.getName(), theStream.getSensor().getName()), topic, key, hosts);
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
        return sender.sendObservation(observation, callback);
    }

 
    @Override
    public boolean send(T value, Instant when, Callback callback) {
        Observation observation = new Observation();
        observation.setDatastream(theStream.withOnlyId());
        observation.setPhenomenonTimeFrom(ZonedDateTime.from(when));
        observation.setResult(value);
        observation.setValidTime(Interval.of(Instant.now(), Duration.ofSeconds(30)));
        return sender.sendObservation(observation, callback);
    }

    @Override
    public boolean send(T value, Instant when, Duration duration, Callback callback) {
        Observation observation = new Observation();
        observation.setDatastream(theStream.withOnlyId());
        observation.setPhenomenonTimeFrom(Interval.of(when, duration));
        observation.setResult(value);
        observation.setValidTime(Interval.of(Instant.now(), Duration.ofSeconds(30)));
        return sender.sendObservation(observation, callback);
    }

    @Override
    public void close() {
        if ( sender != null && !sender.isClosed()) {
            sender.close();
        }
    }
}
