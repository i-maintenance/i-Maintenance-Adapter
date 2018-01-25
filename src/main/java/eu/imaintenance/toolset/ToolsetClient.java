package eu.imaintenance.toolset;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import eu.imaintenance.toolset.api.ObservationHandler;
import eu.imaintenance.toolset.api.Producer;
import eu.imaintenance.toolset.helper.JSON;
import eu.imaintenance.toolset.helper.KafkaSetting;
import eu.imaintenance.toolset.kafka.Consumer;
import eu.imaintenance.toolset.kafka.ObservationSender;
import eu.imaintenance.toolset.measurement.OMMeasurementHandler;
import eu.imaintenance.toolset.measurement.OMTruthObservation;
import eu.imaintenance.toolset.observation.ObservationProcessor;
import eu.imaintenance.toolset.observation.ObservationType;

public class ToolsetClient {
    private Logger logger = LoggerFactory.getLogger(ToolsetClient.class);
    /**
     * Service Object connected to the SensorThings-Server
     */
    private SensorThingsService service;
    /**
     * Runnable class listening to the kafka topics
     */
    private List<Consumer> consumers = new ArrayList<Consumer>();
    
    private ObservationProcessor observationProcessor;
    
    private Map<Long, Class<?>> adapter = new HashMap<Long, Class<?>>();
    private Map<Class<?>, ObservationHandler<?> > handlers = new HashMap<Class<?>, ObservationHandler<?>>();
    private String clientName;
    
    private List<String> kafkaHosts = new ArrayList<String>();
    private List<String> topics = new ArrayList<String>();

    public ToolsetClient() {
        // default
    }
    public ToolsetClient(String serviceUri) throws MalformedURLException, URISyntaxException {
        service = new SensorThingsService(URI.create(serviceUri).toURL());
    }

    public ToolsetClient withServiceUri(String serviceUri) throws MalformedURLException, URISyntaxException {
        service = new SensorThingsService(URI.create(serviceUri).toURL());
        return this;
    }
    public ToolsetClient forThing(Long thingId) throws ServiceFailureException {
        if (service == null ) {
            throw new IllegalStateException("No service URI set - use withServiceUri before!");
        }
        Thing theThing = service.things().find(thingId);
        processKafkaSettings(theThing);
        // create the basic observation adapter for the observed thing!
        observationProcessor = new ObservationProcessor(theThing);
        return this;

    }
    /**
     * Helper method for adding new (non-configured) kafka hosts
     * @param host
     * @return
     */
    public ToolsetClient withHosts(String ... host) {
        for( String h : host ) {
            if (!kafkaHosts.contains(h)) {
                logger.info("adding {} to the list of Kafka Hosts ...", h);
                kafkaHosts.add(h);
            }
        }
        return this;
    }
    /**
     * Helper method for adding new (non-configured) topics
     * @param topicList
     * @return
     */
    public ToolsetClient observeTopics(String ... topicList) {
        for( String h : topicList ) {
            if (!topics.contains(h)) {
                logger.info("adding {} to the list of watched topics ...", h);
                topics.add(h);
            }
        }
        return this;
    }
    public ToolsetClient registerObservationHanlder(ObservationHandler<?> handler, String...datastreamNames) {
        if (observationProcessor == null ) {
            throw new IllegalStateException("No service URI set - use withServiceUri before!");
        }
        if ( datastreamNames == null || datastreamNames.length == 0 ) {
            observationProcessor.registerHandler(handler);
        }
        else {
            for ( String id : datastreamNames ) {
                observationProcessor.registerHandler(handler, id);
            }

        }
        return this;
    }
    public ToolsetClient registerObservationHandler(ObservationHandler<?> handler, Long ... datastreams) {
        if (observationProcessor == null ) {
            throw new IllegalStateException("No service URI set - use withServiceUri before!");
        }
        if ( datastreams == null || datastreams.length == 0 ) {
            observationProcessor.registerHandler(handler);
        }
        else {
            for ( Long id : datastreams ) {
                observationProcessor.registerHandler(handler, id);
            }
        }
        return this;
    }
    
    public ToolsetClient setName(String name) {
        this.clientName = name;
        return this;
    }
    public void sendObservation(Long stream, Object observation) {
        
    }
    public <T> Producer<T> createProducer(Long streamId, String topic, Class<T> resultType) throws ServiceFailureException {
        if (!topics.contains(topic)) {
            throw new ServiceFailureException(String.format("Unknown Topic [%s]: Allowed values ->[%s]!", topic, String.join(", ", topics) ));
        }
        Datastream stream = observationProcessor.getDatastream(streamId);
        // 
        ObservationType obsType = ObservationType.fromString(stream.getObservationType());
        switch (obsType) {
        case MEASUREMENT:
        case COUNT_OBSERVATION:
        case CATEGORY_OBSERVATION:
        case TRUTH_OBSERVATION:
            if (! resultType.equals(obsType.getObservedType())) {
                throw new ServiceFailureException(String.format("Incompatible Types: Stream %s (%s) requires %s as it's data type!", stream.getName(), stream.getId(), obsType.getObservedType().getSimpleName() ));
            }
            break;
        default:
            break;
            
        }
        ObservationSender<T> prod = new ObservationSender<T>(clientName, stream, topic, null, kafkaHosts);
        return (Producer<T>) prod;
    }
    public void startup() {
        int numConsumers = 3;
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            // 
            Consumer consumer = new Consumer(i, clientName, kafkaHosts, topics, observationProcessor);
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
    
    public void shutdown() {
        // 
        System.exit(0);
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
                    if (!this.kafkaHosts.contains(h)) {
                        logger.info("adding {} to the list of Kafka Hosts ...", h);
                        this.kafkaHosts.add(h);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage());
            }
        }
        
    }
    /**
     * Demonstrating the use of MaintenanceClient
     * @param args
     * @throws MalformedURLException
     * @throws URISyntaxException
     * @throws ServiceFailureException
     */
    public static void main(String [] args) throws MalformedURLException, URISyntaxException, ServiceFailureException {
        ToolsetClient client = new ToolsetClient()
            .withServiceUri("http://il060:8082/v1.0/")
            .forThing(1l)
            .setName("ToolsetClient");
        
        // register a handler which is only invoked on datastreams 2, 3
        client.registerObservationHandler(new OMMeasurementHandler(), 2l, 3l);
        // register a handler which is invoked on all measurements (expecting double)
        client.registerObservationHandler(new OMMeasurementHandler());
        // register a handler wich is invoked on the datastream named "Temp511"
        client.registerObservationHanlder(new OMTruthObservation(), "Temp511");
        // start the message listener
        client.startup();
        
    }
}
