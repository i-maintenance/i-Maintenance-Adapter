package eu.imaintenance.toolset;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import eu.imaintenance.toolset.api.ObservationHandler;
import eu.imaintenance.toolset.api.Producer;
import eu.imaintenance.toolset.measurement.OMMeasurementHandler;
import eu.imaintenance.toolset.measurement.OMTruthObservationHandler;
import eu.imaintenance.toolset.observation.ObservationProcessor;

public class ToolsetClient {
    private Logger logger = LoggerFactory.getLogger(ToolsetClient.class);
    /**
     * Service Object connected to the SensorThings-Server
     */
    private SensorThingsService service;

    /**
     * Runnable class listening to the kafka topics
     */
   
    private Map<Long, ObservationProcessor> processor = new HashMap<Long, ObservationProcessor>();
    
    private String clientName = UUID.randomUUID().toString();
    
    /**
     * Default constructor. Creates <i>empty</i> client. The <b>mandatory</b> service uri must be set
     * with {@link ToolsetClient#withServiceUri(String)} before further usage!
     */
    public ToolsetClient() {
        // default
    }
    /**
     * Constructor taking a URI pointing to the SensorThings server.
     * @param serviceUri
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    public ToolsetClient(String serviceUri) throws MalformedURLException, URISyntaxException {
        service = new SensorThingsService(URI.create(serviceUri).toURL());
    }
    /**
     * Specify the SensorThings Service URI
     * @param serviceUri
     * @return
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    public ToolsetClient withServiceUri(String serviceUri) throws MalformedURLException, URISyntaxException {
        service = new SensorThingsService(URI.create(serviceUri).toURL());
        return this;
    }
    /**
     * Methond creating 
     * @param thingIds
     * @return
     * @throws ServiceFailureException
     */
    public ToolsetClient forThing(Long ... thingIds) throws ServiceFailureException {
        if (service == null ) {
            throw new IllegalStateException("No service URI set - use withServiceUri before!");
        }
        for (Long thingId : thingIds) {
            Thing theThing = service.things().find(thingId);
            if ( theThing == null ) {
                throw new IllegalStateException(String.format("Thing(%s) not found!", thingId));
            }
            // register the observation processor
            registerObservationProcessor(theThing);
        }
        return this;

    }
    public ToolsetClient forThing(String thingName) throws ServiceFailureException {
        if (service == null ) {
            throw new IllegalStateException("No service URI set - use withServiceUri before!");
        }
        EntityList<Thing> things = service.things().query().filter(String.format("%s eq '%s'", "name", thingName)).list();
        if ( things.size() > 1) {
            // in case there are more "things with the same name!
            throw new ServiceFailureException("The name of the thing is ambigious! - use the respective ID's ");
        }
        Iterator<Thing> thingIterator = things.iterator();
        while ( thingIterator.hasNext() ) {
            registerObservationProcessor(thingIterator.next());
        }
        return this;
    }
    public ToolsetClient registerHandler(Long thingId, ObservationHandler<?> handler) throws ServiceFailureException {
        Thing theThing = service.things().find(thingId);
        ObservationProcessor proc = registerObservationProcessor(theThing);
        proc.registerHandler(handler);
        return this;
    }
    public ToolsetClient registerHandler(String thingName, ObservationHandler<?> handler) throws ServiceFailureException {
        Iterator<Thing> thingIterator = service.things().query().filter(filterEquals("name",thingName)).list().iterator();
        while (thingIterator.hasNext()) {
            Thing theThing = thingIterator.next();
            ObservationProcessor proc = registerObservationProcessor(theThing);
            proc.registerHandler(handler);
        }
        return this;
    }
    public ToolsetClient registerHandler(Thing theThing, ObservationHandler<?> handler, Long ...datastreamIds ) throws ServiceFailureException {
        ObservationProcessor proc = registerObservationProcessor(theThing);
        if ( datastreamIds == null || datastreamIds.length == 0) {
            proc.registerHandler(handler);
        }
        else {
            for (Long datastreamId : datastreamIds) {
                Datastream stream = theThing.datastreams().find(datastreamId);
                proc.registerHandler(handler, stream);
            }
        }
        return this;
    }
    public ToolsetClient registerHandler(Thing theThing, ObservationHandler<?> handler, String ...datastreamNames ) throws ServiceFailureException {
        ObservationProcessor proc = registerObservationProcessor(theThing);
        if ( datastreamNames == null || datastreamNames.length == 0) {
            proc.registerHandler(handler);
        }
        else {
            for (String datastreamName : datastreamNames) {
                
                Iterator<Datastream> streamIterator = theThing.datastreams().query().filter(filterEquals("name", datastreamName)).list().iterator();
                while (streamIterator.hasNext()) {
                    Datastream stream = streamIterator.next();
                    proc.registerHandler(handler, stream);
                }
            }
        }
        return this;
    }
    public ToolsetClient registerHandler(Long thingId, ObservationHandler<?> handler, Long ...datastreamIds ) throws ServiceFailureException {
        Thing theThing = service.things().find(thingId);
        if ( theThing != null ) {
            return registerHandler(theThing, handler, datastreamIds);
        }
        return this;
    }
    public ToolsetClient registerHandler(Long thingId, ObservationHandler<?> handler, String ...datastreamNames ) throws ServiceFailureException {
        Thing theThing = service.things().find(thingId);
        if ( theThing != null ) {
            return registerHandler(theThing, handler, datastreamNames);
        }
        return this;
    }
    public ToolsetClient registerHandler(String thingName, ObservationHandler<?> handler, Long ...datastreamIds ) throws ServiceFailureException {
        Iterator<Thing> thingIterator = service.things().query().filter(filterEquals("name",thingName)).list().iterator();
        while (thingIterator.hasNext()) {
            Thing theThing = thingIterator.next();
            registerHandler(theThing, handler, datastreamIds);
        }
        return this;
    }
    public ToolsetClient registerHandler(String thingName, ObservationHandler<?> handler, String ...datastreamNames ) throws ServiceFailureException {
        Iterator<Thing> thingIterator = service.things().query().filter(filterEquals("name",thingName)).list().iterator();
        while (thingIterator.hasNext()) {
            Thing theThing = thingIterator.next();
            registerHandler(theThing, handler, datastreamNames);
        }
        return this;
    }
    /**
     * Register an observation handler to be invoked for the provided 
     * @param handler
     * @param datastreamNames
     * @return
     * @throws ServiceFailureException
     */
    public ToolsetClient registerHandler(ObservationHandler<?> handler, String...datastreamNames) throws ServiceFailureException {

        for ( String name : datastreamNames ) {
            Iterator<Datastream> streamIterator = service.datastreams().query().filter(filterEquals("name", name)).list().iterator();
            while (streamIterator.hasNext()) {
                Datastream stream = streamIterator.next();
                ObservationProcessor proc = registerObservationProcessor(stream.getThing());
                proc.registerHandler(handler, stream);
                
            }
        }
        return this;
    }
    /**
     * Register an {@link ObservationHandler} for the provided datastream id's
     * @param handler
     * @param datastreams
     * @return
     * @throws ServiceFailureException
     */
    public ToolsetClient registerHandler(ObservationHandler<?> handler, Long ... datastreams) throws ServiceFailureException {
        for ( Long id : datastreams ) {
            Datastream stream = service.datastreams().find(id);
            ObservationProcessor proc = registerObservationProcessor(stream.getThing());
            proc.registerHandler(handler, stream);
        }
        return this;
    }
    
    public ToolsetClient setName(String name) {
        this.clientName = name;
        return this;
    }
    
    public <T> Producer<T> createProducer(Long streamId, String topic, Class<T> resultType) throws ServiceFailureException {
        Datastream stream = service.datastreams().find(streamId);
        if ( stream == null ) {
            throw new ServiceFailureException(String.format("Datastream(%s) not found!", streamId));
        }
        Thing forThing = stream.getThing();
        ObservationProcessor proc = registerObservationProcessor(forThing);
        return proc.createProducer(stream, topic, resultType);
    }

    public void startup() {
        for ( Long tId : processor.keySet()) {
            processor.get(tId).startup(clientName);
        }
    }
    
    public void shutdown() {
        // 
        System.exit(0);
    }
    /**
     * Helper method formatting sensor things filters
     * @param name
     * @param value
     * @return
     * @throws ServiceFailureException
     */
    private String filterEquals(String name, String value) throws ServiceFailureException {
        return String.format("%s eq '%s'", name, value);
    }
    /**
     * Creates and registers the internal {@link ObservationProcessor}
     * @param aThing
     * @return
     */
    private ObservationProcessor registerObservationProcessor(Thing aThing) {
        // register only once
        if (! processor.containsKey(aThing.getId())) {
            ObservationProcessor thingProcessor = new ObservationProcessor(aThing);
            processor.put(aThing.getId(), thingProcessor);
            return thingProcessor;
        }
        return processor.get(aThing.getId());
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
        client.registerHandler(new OMMeasurementHandler(), 2l, 3l);
        // register a handler which is invoked on all measurements (expecting double)
//        client.registerHandlerByName(new OMMeasurementHandler());
        client.registerHandler(1l, new OMMeasurementHandler());
        // register a handler wich is invoked on the datastream named "Temp511"
        client.registerHandler(new OMTruthObservationHandler(), "Temp511");
        // start the message listener
        client.startup();
        

        
    }
}
