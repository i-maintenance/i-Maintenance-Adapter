package eu.imaintenance.toolset.observation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.api.ObservationHandler;
import eu.imaintenance.toolset.observation.ObservationType;

/**
 * Example implementation for a {@link ObservationType#TRUTH_OBSERVATION} handler.
 * This particular observation specifies {@link Boolean} as it's type.
 * 
 * @author dglachs
 *
 */
public class OMTruthObservationHandler implements ObservationHandler<Boolean> {
    Logger logger = LoggerFactory.getLogger(OMMeasurementHandler.class);

    @Override
    public Class<Boolean> getObservedType() {
        return Boolean.class;
    }

    @Override
    public void onObservation(Observation observation, Boolean result) {
        try {
            logger.info(observation.getDatastream().getName() + " - " + observation.getPhenomenonTime().toString() + ": " + result);
        } catch (ServiceFailureException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
