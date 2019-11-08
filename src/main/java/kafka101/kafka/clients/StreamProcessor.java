package kafka101.kafka.clients;


import kafka101.events.Event;
import kafka101.events.EventType;
import kafka101.kafka.utils.EventDeserializer;
import kafka101.kafka.utils.EventSerializer;
import kafka101.kafka.configuration.KafkaConfiguration;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

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

        final KafkaStreams kafkaStreams = new KafkaStreams(kafkaStreamTopology(), props);
        kafkaStreams.start();

        return kafkaStreams;
    }


    @Bean
    public Topology kafkaStreamTopology(){
        Serde<Event> eventSerde = Serdes.serdeFrom(new EventSerializer(), new EventDeserializer());
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<Integer, Event> source = streamsBuilder.stream(KafkaConfiguration.TOPIC_NAME, Consumed.with(Serdes.Integer(), eventSerde));
        KTable<Integer,Integer> accountBalance = source.map((key,value) -> new KeyValue<Integer,Integer>(key, isWithdraw(value)?-value.getAmount():value.getAmount()))
                                                       .groupByKey()
                                                       .aggregate(  // Java 8+, using Lambda expressions
                                                           () -> 0,
                                                           (key,value,balance) -> balance + value,
                                                           Materialized.as(KafkaConfiguration.STREAM_STATE_STORE)
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

    public boolean isWithdraw(Event event){
        EventType type = event.getEventType();
        if (type.equals(EventType.WITHDRAW)){
            return true;
        }
        else{
            return false;
        }
    }
}
