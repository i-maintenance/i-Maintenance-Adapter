package eu.imaintenance.toolset.observation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.api.ObservationHandler;
import eu.imaintenance.toolset.observation.AbstractObservationHandler;
import eu.imaintenance.toolset.observation.ObservationType;
/**
 * Example implementation for an {@link ObservationType#MEASUREMENT} handler.
 * This handler extends {@link AbstractObservationHandler} thus need not 
 * implement the {@link ObservationHandler#getObservedType()} method.
 * 
 * @author dglachs
 *
 */
public class OMMeasurementHandler extends AbstractObservationHandler<Double> {
    Logger logger = LoggerFactory.getLogger(OMMeasurementHandler.class);

    @Override
    public void onObservation(Observation observation, Double result) {
        try {
            logger.info(observation.getDatastream().getName() + " - " + observation.getPhenomenonTime().toString() + ": " + result);
        } catch (ServiceFailureException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
