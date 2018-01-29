package eu.imaintenance.toolset.observation;

/**
 * Enumeration outlining the available observation types. Any datastream must outline it's 
 * payload type with the <b>observationType</b> attribute. This enumeration maps the 
 * observation type to a Java Class!
 * @author dglachs
 *
 */
public enum ObservationType {
    MEASUREMENT("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", Double.class),
    OBSERVATION("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation", Object.class),
    COUNT_OBSERVATION("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation", Integer.class),
    TRUTH_OBSERVATION("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation", Boolean.class),
    CATEGORY_OBSERVATION("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation", String.class)
    ;
    
    ObservationType(String type, Class<?>clazz) {
        this.type = type;
        this.clazz = clazz;
        
        
    }
    
    private String type;
    private Class<?> clazz;
    
    public Class<?> getObservedType() {
        return clazz;
    }
    public String getType() {
        return type;
    }
    public static ObservationType fromString(String type) {
        for (ObservationType t : ObservationType.values()) {
            if ( t.type.equals(type)) {
                return t;
            }
        }
        // use OBSERVATION as Default (generic)
        return OBSERVATION;
    }
    public static ObservationType fromObservedClass(Class<?> clazz) {
        for (ObservationType t : ObservationType.values()) {
            if ( t.clazz.equals(clazz)) {
                return t;
            }
        }
        return OBSERVATION;
    }
    
}
