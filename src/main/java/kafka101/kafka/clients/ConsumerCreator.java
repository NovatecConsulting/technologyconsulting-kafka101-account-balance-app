package kafka101.kafka.clients;

import com.fasterxml.jackson.databind.JsonNode;

import kafka101.kafka.configuration.KafkaConfiguration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.web.context.WebApplicationContext;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class ConsumerCreator {
    @Bean
    @Scope(value= WebApplicationContext.SCOPE_REQUEST, proxyMode= ScopedProxyMode.TARGET_CLASS)
    public Consumer<Integer, JsonNode> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.KAFKA_BROKERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConfiguration.CONSUMER_CLIENT_ID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfiguration.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConfiguration.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfiguration.OFFSET_RESET_EARLIER);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Consumer<Integer, JsonNode> consumer = new KafkaConsumer<Integer, JsonNode>(props);
        consumer.subscribe(Collections.singletonList(KafkaConfiguration.TOPIC_NAME));

        return consumer;
    }
}
