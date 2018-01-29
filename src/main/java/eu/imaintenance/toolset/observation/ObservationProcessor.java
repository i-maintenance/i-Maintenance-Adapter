package eu.imaintenance.toolset.observation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import eu.imaintenance.toolset.api.ObservationHandler;
import eu.imaintenance.toolset.api.Producer;
import eu.imaintenance.toolset.helper.JSON;
import eu.imaintenance.toolset.helper.KafkaSetting;
import eu.imaintenance.toolset.helper.ResultHelper;
import eu.imaintenance.toolset.kafka.Consumer;
/**
 * Helper class performing the mapping of datastreams and their id's
 * @author dglachs
 *
 */
public final class ObservationProcessor {
    private Logger logger = LoggerFactory.getLogger("ObservationParser");
    /**
     * Observation must be performed for a particular "thing"
     */
    private final Thing theThing;
    /**
     * List of registered datastreams
     */
    private Map<Long, Datastream> registeredDatastream = new HashMap<Long, Datastream>();
    /**
     * List of handlers assigned to a distinct datastream
     */
    private Map<Long, ObservationHandler<?>> registeredHandler = new HashMap<Long, ObservationHandler<?>>();
    /**
     * List of collected datastreams ...
     */
    private Map<Long, Datastream> collectedDatastream = new HashMap<Long, Datastream>();
    /**
     * List of registered datastream types ...
     */
    private Map<Long, Class<?>> registeredDatastreamType = new HashMap<Long, Class<?>>();
    /**
     * Mapping of observation types and ObservationHandlers
     */
    private Map<Class<?>, ObservationHandler<?> > typedHandler = new HashMap<Class<?>, ObservationHandler<?>>();
    
    private List<Consumer> consumers = new ArrayList<Consumer>();
    
    private List<String> hosts = new ArrayList<String>();
    private List<String> topics = new ArrayList<String>();
    
    public ObservationProcessor(Thing theThing) {
        this.theThing = theThing;
        processKafkaSettings(this.theThing);
    }
    public ObservationProcessor(Thing theThing, List<String> hosts, List<String> topics) {
        this.theThing = theThing;
        this.hosts = hosts !=null ? hosts: new ArrayList<String>();
        this.topics = topics != null ? topics : new ArrayList<String>();
        processKafkaSettings(this.theThing);
        
    }
    /**
     * Register a {@link ObservationHandler} with the given datastream id
     * @param streamId
     * @param handler
     */
    public <T> void registerHandler(ObservationHandler<T> handler, Long streamId) {
        try {
            Datastream stream = getDatastream(streamId);
            if ( stream != null ) {
                registerDatastream(stream, handler);
            }
            else {
                logger.info("The requested datastream {} cannot be found ... handler not registered!", streamId);
            }

        } catch (ServiceFailureException e) {
            logger.error(e.getLocalizedMessage());
        }
        
    }
    public <T> void registerHandler(ObservationHandler<T> handler, Datastream stream) throws ServiceFailureException {
        // register only if steam belongs to the processors thing
        if ( stream.getThing().getId()== theThing.getId()) {
            registerDatastream(stream, handler);
        }
    }
    public <T> void registerHandler(ObservationHandler<T> handler, String name) {
        try {
            EntityList<Datastream> streams = theThing.datastreams().query().filter(String.format("%s eq '%s'", "name",name)).list();
            Iterator<Datastream> iterator = streams.iterator();
            while (iterator.hasNext() ) {
                Datastream stream = iterator.next();
                registerDatastream(stream, handler);
            }
        } catch (ServiceFailureException e) {
            logger.error(e.getLocalizedMessage());
        }
        
    }
    public void startup(String clientName) {
        int numConsumers = 3;
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            // 
            
            Consumer consumer = new Consumer(i, String.format("%s: %s", clientName, theThing.getName()), hosts, topics, this);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (Consumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    /**
     * Helper method to retrieve a datastream. If not found a {@link ServiceFailureException} is thrown.
     * @param streamId The stream to obtain
     * @return
     * @throws ServiceFailureException
     */
    public Datastream getDatastream(Long streamId) throws ServiceFailureException {
        Datastream s = registeredDatastream.get(streamId); 
        if ( s == null ) {
            s = collectedDatastream.get(streamId);
        }
        if ( s == null ) {
            s = theThing.datastreams().find(streamId);
        }
        if ( s == null ) {
            throw new ServiceFailureException(String.format("Datastream with id %s is not available!", streamId));
        }
        // keep the thing if 
        if ( s.getThing()==null) {
            s.setThing(theThing);
        }
        return s;
    }
    /**
     * Register a {@link ObservationHandler} along with a given {@link Datastream}.
     * 
     * Whenever the i-Maintenance-System processes an observation for the given
     * {@link Datastream}, the respective handler is used to process the payload!
     * @param stream The {@link Datastream} 
     * @param handler The observation handler object
     */
    private <T> void registerDatastream(Datastream stream, ObservationHandler<T> handler) {
        
        // keep the stram in the registred map
        registeredDatastream.put(stream.getId(), stream);
        // keep the handler mapped with the stream id
        registeredHandler.put(stream.getId(), handler);
        registeredDatastreamType.put(stream.getId(), handler.getObservedType());
        // 

    }
    /**
     * Register 
     * @param handler
     */
    public <T> void registerHandler(ObservationHandler<T> handler) {
        Class<T> clazz = handler.getObservedType();
        if (! typedHandler.containsKey(clazz)) {
            typedHandler.put(clazz, handler);
        }
    }
    /**
     * Raw method processing Kafka messages, the payload first transformed to 
     * an {@link Observation} and then the payload (e.g. the result) is further
     * examined and the respective handler is invoked
     * @param topic
     * @param key
     * @param payload
     * @throws ServiceFailureException 
     */
    public void processKafkaMessage(String topic, String key, String payload) throws ServiceFailureException {
        try {
            Observation observation = JSON.deserializeFromString(payload, Observation.class);
            Long streamId = observation.getDatastream().getId();
            if (registeredDatastream != null && registeredDatastream.containsKey(streamId)) {
                // found the stream in the registered map
                observation.setDatastream(registeredDatastream.get(streamId));
                // use the handler registred with the stream
                ObservationHandler<?> handler = registeredHandler.get(streamId);
                //
                if ( handler != null) {
                    // transform the payload into the requested object
                    handlePayloadTyped(handler, observation);
                }
            }
            else {
                // 
                if ( typedHandler != null && typedHandler.size() > 0 ) {
                    Datastream stream = getFromCollectedDatastreams(observation.getDatastream().getId());
                    if ( stream != null) {
                        observation.setDatastream(stream);
                    
                        ObservationType obType = ObservationType.fromString(stream.getObservationType());
                        ObservationHandler<?> handler = typedHandler.get(obType.getObservedType());
                        if ( handler != null ) {
                            handlePayloadTyped(handler, observation);
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Payload of topic {}, key {} cannot be parsed as an observation: {}", topic, key, payload);

        }
        
        
    }
    private Datastream getFromCollectedDatastreams(Long id) throws ServiceFailureException  {
        Datastream stream = collectedDatastream.get(id);
        if ( stream == null ) {
            stream = theThing.datastreams().find(id);
            if (stream != null) {
                stream.setThing(theThing);
                collectedDatastream.put(stream.getId(), stream);
            }
            else {
                // stream not part of this "thing"
                logger.info("The requested datastream {} cannot be found ... payload not processed!", id);
            }
        }
        return stream;
        
    }
    /**
     * Transform the payload object into it's typed representation and call the {@link ObservationHandler#onObservation(Observation, Object)}
     * method!
     * @param handler
     * @param observation
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    private <T> void handlePayloadTyped(ObservationHandler<T> handler, Observation observation) throws JsonParseException, JsonMappingException, IOException {
        Object payload = observation.getResult();
        if ( payload instanceof Number) {
            Number n = (Number) payload;
            if ( handler.getObservedType().equals(Integer.class)) {
                handler.onObservation(observation, handler.getObservedType().cast(n.intValue()));
            }
            if ( handler.getObservedType().equals(Double.class)) {
                handler.onObservation(observation, handler.getObservedType().cast(n.doubleValue()));
            }
            if ( handler.getObservedType().equals(Boolean.class)) {
                handler.onObservation(observation, handler.getObservedType().cast(n.intValue()==1));
            }
        }
        else if( payload instanceof Boolean) {
            handler.onObservation(observation, handler.getObservedType().cast(observation.getResult()));
        }
        else if (payload instanceof String) {
            handler.onObservation(observation, handler.getObservedType().cast(observation.getResult()));
        }
        else if (payload instanceof Map){
            ResultHelper helper = new ResultHelper(payload);
            T typed = JSON.deserializeFromString(helper.toString(), handler.getObservedType()); 
            handler.onObservation(observation, typed);
        }
    }
    private void processKafkaSettings(Thing theThing) {
        Map<String, Object> props = theThing.getProperties();
        if ( props.get("kafka") != null ) {
            KafkaSetting k;
            try {
                k = JSON.deserializeFromString(props.get("kafka"), KafkaSetting.class);
                if ( !this.topics.contains(k.topics.get("sensor"))) {
                    logger.info("adding {} to the list of watched topics ...", k.topics.get("sensor"));
                    this.topics.add(k.topics.get("sensor"));
                }
                if ( !this.topics.contains(k.topics.get("alert"))) {
                    logger.info("adding {} to the list of watched topics ...", k.topics.get("alert"));
                    this.topics.add(k.topics.get("alert"));
                }
                for ( String h : k.hosts) {
                    if (!this.hosts.contains(h)) {
                        logger.info("adding {} to the list of Kafka Hosts ...", h);
                        this.hosts.add(h);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage());
            }
        }
        
    }
    public boolean verifyTopic(String topic) throws ServiceFailureException {
        if (! this.topics.contains(topic)) {
            throw new ServiceFailureException(String.format("Topic [%s] is not supported for [%s]!", topic, theThing.getName()));
        }
        return true;
    }
    public boolean verifyDatastreamType(Datastream stream, Class<?> clazz) throws ServiceFailureException {
        ObservationType obsType = ObservationType.fromString(stream.getObservationType());
        if (! obsType.getObservedType().isAssignableFrom(clazz)) {
            throw new ServiceFailureException(
                    String.format("Incompatible Types: Stream %s (%s) requires %s as it's data type!", stream.getName(), stream.getId(), obsType.getObservedType().getSimpleName() ));
        }
        return true;
    }
    public <T> Producer<T> createProducer(Datastream stream, String topic, Class<T> resultType ) throws ServiceFailureException {
        verifyTopic(topic);
        verifyDatastreamType(stream, resultType);
        ObservationSender<T> s = new ObservationSender<T>(theThing.getName(), stream, topic, null, this.hosts);
        return (Producer<T>)s;

    }

}
