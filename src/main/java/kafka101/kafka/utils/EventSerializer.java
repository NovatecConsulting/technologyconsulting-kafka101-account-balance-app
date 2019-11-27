package kafka101.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka101.events.Event;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Deprecated
public class EventSerializer implements Serializer<Event> {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Event data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
            System.out.println("Error in serializing object"+ data);
        }
        return retVal;
    }

    public void close() {
    }
}
