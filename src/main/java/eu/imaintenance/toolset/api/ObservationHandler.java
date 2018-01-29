package eu.imaintenance.toolset.api;

import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import eu.imaintenance.toolset.observation.AbstractObservationHandler;
/**
 * Interface to be implemented by participating applications
 * @author dglachs
 *
 * @param <T>
 */
public interface ObservationHandler<T> {
    /**
     * Return the expected Java Class Type of the payload. 
     * {@link AbstractObservationHandler#getObservedType()} provides
     * a default implementation and serves as a base class for 
     * your own {@link ObservationHandler}
     * 
     * @return
     */
    public Class<T> getObservedType();
    
    /**
     * Method invoked when the {@link ObservationHandler} receives 
     * a message from the underlying messsaging ecosystem.
     * @param observation The metadata for the observation, see {@link Observation}. 
     * The observation provides access to the {@link Datastream} and the 
     * observed {@link Thing}.
     * @param result The payload of the messsage. 
     */
    public void onObservation(Observation observation, T result);
    
}
