package kafka101.kafka;

import kafka101.events.DepositEvent;
import kafka101.events.Event;
import kafka101.events.WithdrawEvent;
import kafka101.kafka.utils.EventSerializer;
import kafka101.kafka.configuration.KafkaConfiguration;
import kafka101.kafka.clients.StreamProcessor;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class TestProcessorTopology {

    private TopologyTestDriver testDriver;
    ReadOnlyKeyValueStore<Integer, Integer> store;

    @Before
    public void setUp(){
        StreamProcessor processor = new StreamProcessor();
        Topology topology = processor.kafkaStreamTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        testDriver = new TopologyTestDriver(topology, props);
        store = testDriver.getKeyValueStore(KafkaConfiguration.STREAM_STATE_STORE);
    }

    @After
    public void tearDown(){
        //testDriver.close(); //Unresolved bug on windows
        testDriver = null;
        store = null;
    }

    @Test
    public void testTopology(){
        ConsumerRecordFactory<Integer, Event> factory = new ConsumerRecordFactory<>(KafkaConfiguration.TOPIC_NAME,  new IntegerSerializer(), new EventSerializer());

        testDriver.pipeInput(factory.create(1, new WithdrawEvent(1, 100)));
        int amount = store.get(1);
        Assert.assertEquals(-100,amount);


        testDriver.pipeInput(factory.create(1, new DepositEvent(1, 100)));
        amount = store.get(1);
        Assert.assertEquals(0,amount);
    }


}
