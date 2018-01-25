package eu.imaintenance.toolset.api;

import de.fraunhofer.iosb.ilt.sta.model.Observation;
/**
 * Interface to be implemented by participating applications
 * @author dglachs
 *
 * @param <T>
 */
public interface ObservationHandler<T> {
    
    public Class<T> getObservedType();
    
    
    public void onObservation(Observation observation, T result);
    
}
