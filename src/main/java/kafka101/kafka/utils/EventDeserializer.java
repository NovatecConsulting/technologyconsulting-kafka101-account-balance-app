package kafka101.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka101.events.Event;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Deprecated
public class EventDeserializer implements Deserializer<Event> {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public Event deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Event object = null;
        try {
            object = mapper.readValue(data, Event.class);

        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes "+ exception);
        }
        return object;
    }

    public void close() {
    }
}
