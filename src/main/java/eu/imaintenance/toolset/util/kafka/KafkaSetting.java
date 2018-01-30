package eu.imaintenance.toolset.util.kafka;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class KafkaSetting {
    public List<String> hosts;
    public Map<String, String> topics;
}
