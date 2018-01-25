package eu.imaintenance.toolset.helper;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ResultHelper extends LinkedHashMap<Object, Object> {
    
    /**
     * generated serial
     */
    private static final long serialVersionUID = 7695140535137683733L;
    
    public ResultHelper(Map<?, ?> map) {
        putAll(map);
    }
    public ResultHelper(Object o) {
        if ( o instanceof Map) {
            Map<?, ?> oMap = (Map<?, ?>) o;
            for (Object oKey : oMap.keySet()) {
                Object val = oMap.get(oKey);
                if ( val instanceof Map) {
                    put(oKey.toString(), new ResultHelper(val));
                }
                else {
                    put(oKey.toString(), val);
                }
            }
        }
    }
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        Iterator<Object> keyIterator = keySet().iterator();
        while ( keyIterator.hasNext() ) {
            Object key = keyIterator.next();
            builder.append(String.format("\"%s\"", key));
            builder.append(":");
            Object val = get(key);
            if ( val instanceof String ) {
                builder.append(String.format("\"%s\"", val));
            }
            else if ( val instanceof List ) {
                @SuppressWarnings("unchecked")
                Iterator<Object> listIterator = ((List)val).iterator();
                builder.append("[");
                while (listIterator.hasNext()) {
                    Object lVal = listIterator.next();
                    if ( lVal instanceof String) {
                        builder.append(String.format("\"%s\"", lVal));
                    }
                    else if ( lVal instanceof Map) {
                        builder.append(new ResultHelper(val).toString());
                    }
                    else {
                        builder.append(lVal);
                    }
                    if (listIterator.hasNext()) {
                        builder.append(",");
                    }
                    
                }
                builder.append("]");
            }
            else if (val instanceof Map) {
                builder.append(new ResultHelper(val).toString());
            }
            else {
                builder.append(val.toString());
            }
            if ( keyIterator.hasNext()) {
                builder.append(",");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
