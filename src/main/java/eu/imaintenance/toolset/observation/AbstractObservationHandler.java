package eu.imaintenance.toolset.observation;

import java.lang.reflect.ParameterizedType;

import eu.imaintenance.toolset.api.ObservationHandler;

/**
 * Abstract ObservationHandler, to be extended by partners
 * @author dglachs
 *
 * @param <T> The (expected) type of the observation processed/handled
 */
public abstract class AbstractObservationHandler<T> implements ObservationHandler<T> {
    
    Class<T> clazz;
    /**
     * Default implementation extracting the generic type of the {@link ObservationHandler}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getObservedType() {
        ParameterizedType genericSuperclass = (ParameterizedType) getClass().getGenericSuperclass();
        return (Class<T>) genericSuperclass.getActualTypeArguments()[0];
    }
}
