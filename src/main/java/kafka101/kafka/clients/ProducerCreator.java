package kafka101.kafka.clients;


import com.fasterxml.jackson.databind.JsonNode;

import kafka101.kafka.configuration.KafkaConfiguration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class ProducerCreator {

    @Bean
    public Producer<Integer, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfiguration.PRODUCER_CLIENT_ID);

        return new KafkaProducer<Integer, JsonNode>(props);
    }
}
