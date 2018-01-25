package eu.imaintenance.toolset.measurement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import eu.imaintenance.toolset.api.ObservationHandler;

public class OMTruthObservation implements ObservationHandler<Boolean> {
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
