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
    
    /**
     * Default implementation extracting the generic type of the {@link ObservationHandler}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getObservedType() {
        ParameterizedType genericSuperclass = (ParameterizedType) getClass().getGenericSuperclass();
        return (Class<T>) genericSuperclass.getActualTypeArguments()[0];
    }
    /**
     * Retrieve the {@link ObservationType} enumeration based on the handler's observed type! E.g. 
     * <ul>
     * <li>{@link ObservationType#MEASUREMENT} for handlers expecting {@link Double}
     * <li>{@link ObservationType#CATEGORY_OBSERVATION} for handlers expecting {@link String}
     * <li>{@link ObservationType#COUNT_OBSERVATION} for handlers expecting {@link Integer}
     * <li>{@link ObservationType#TRUTH_OBSERVATION} for handlers expecting {@link Boolean}
     * <li>{@link ObservationType#OBSERVATION} for handlers expecting arbitrary {@link Object}
     * </ul>
     * @return
     */
    public ObservationType getObservationType() {
        return ObservationType.fromObservedClass(getObservedType());
    }
}
