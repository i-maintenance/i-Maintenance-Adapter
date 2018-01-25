package eu.imaintenance.toolset.helper;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;

public class JSON {
    
    /**
     * Static helper method to tranform the  
     * @param mapper
     * @param txt
     * @param clazz
     * @return
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static <T> T deserializeFromString(ObjectMapper mapper, String txt, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        T myObjects = mapper.readValue(txt, clazz);
        return myObjects;
    }
    /**
     * Static helper method to tranform the  
     * @param mapper
     * @param txt
     * @param clazz
     * @return
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static <T> T deserializeFromString(String txt, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = ObjectMapperFactory.get();
        T myObjects = mapper.readValue(txt, clazz);
        return myObjects;
        
    }
    /**
     * Static helper method to tranform the  
     * @param mapper
     * @param txt
     * @param clazz
     * @return
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static <T> T deserializeFromString(Object txt, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        if ( txt instanceof Map) {
            ResultHelper result = new ResultHelper(txt);
            return deserializeFromString(result.toString(), clazz);
        }
        T myObjects = ObjectMapperFactory.get().readValue(txt.toString(), clazz);
        return myObjects;
        
    }

}
