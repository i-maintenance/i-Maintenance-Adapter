package eu.imaintenance.toolset.measurement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.observation.AbstractObservationHandler;

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
