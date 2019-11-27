package kafka101.kafka.clients;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka101.events.Event;
import kafka101.events.EventType;
import kafka101.kafka.configuration.KafkaConfiguration;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class StreamProcessor {
    @Bean
    public KafkaStreams kafkaStreams(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfiguration.STREAM_PROCESSOR_CLIENT_ID );
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG,KafkaConfiguration.STREAM_STATE_DIR);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,KafkaConfiguration.STREAM_ENDPOINT);
        final KafkaStreams kafkaStreams = new KafkaStreams(kafkaStreamTopology(), props);
        kafkaStreams.start();

        return kafkaStreams;
    }


    @Bean
    public Topology kafkaStreamTopology(){
        Serde<JsonNode> eventSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();

        KStream<Integer, JsonNode> source = streamsBuilder.stream(KafkaConfiguration.TOPIC_NAME, Consumed.with(Serdes.Integer(), eventSerde));
        KTable<Integer,Integer> accountBalance = source.mapValues(value -> extractAmount(value))
                                                       .groupByKey()
                                                       .aggregate(  // Java 8+, using Lambda expressions
                                                           () -> 0,
                                                           (key,value,balance) -> balance + value,
                                                           Materialized.<Integer,Integer, KeyValueStore<Bytes, byte[]>>as(KafkaConfiguration.STREAM_STATE_STORE)
                                                       );
// Java 7
//                                                .aggregate(new Initializer<Integer>() {
//                                                      @Override
//                                                      public Integer apply() {
//                                                          return 0;
//                                                      }
//                                                  }, new Aggregator<Integer, Integer, Integer>() {
//                                                      @Override
//                                                      public Integer apply(final Integer key, final Integer value, final Integer balance) {
//                                                          return balance + value;
//                                                      }
//                                                  }, Materialized.<Integer,Integer, KeyValueStore<Bytes, byte[]>>as("balance-store"));

        return streamsBuilder.build();
    }

    public int extractAmount(JsonNode jsonNode){
        String eventType = jsonNode.get("eventType").asText();
        int amount = 0;
        if(eventType.equals("WITHDRAW")){
            amount = jsonNode.get("amount").asInt()*(-1);
        }
        if(eventType.equals("DEPOSIT")){
            amount = jsonNode.get("amount").asInt();
        }
        return amount;
    }
}
