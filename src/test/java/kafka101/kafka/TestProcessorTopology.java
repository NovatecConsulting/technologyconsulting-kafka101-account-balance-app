package kafka101.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka101.events.DepositEvent;
import kafka101.events.WithdrawEvent;
import kafka101.kafka.configuration.KafkaConfiguration;
import kafka101.kafka.clients.StreamProcessor;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.connect.json.JsonSerializer;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class TestProcessorTopology {

    private TopologyTestDriver testDriver;
    ReadOnlyKeyValueStore<Integer, Integer> store;
    ObjectMapper objectMapper;

    @Before
    public void setUp(){
        StreamProcessor processor = new StreamProcessor();
        Topology topology = processor.kafkaStreamTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG,"test-state-store");
        testDriver = new TopologyTestDriver(topology, props);
        store = testDriver.getKeyValueStore(KafkaConfiguration.STREAM_STATE_STORE);
        objectMapper = new ObjectMapper();
    }

    @After
    public void tearDown() throws IOException {
        try{
            testDriver.close(); //unresolved bug on windows
        }catch(Exception e){
            FileUtils.deleteDirectory(new File("test-state-store")); //workaround: manually delete the state store folder of the test
        }
    }

    @Test
    public void testTopology(){
        ConsumerRecordFactory<Integer, JsonNode> factory = new ConsumerRecordFactory<>(KafkaConfiguration.TOPIC_NAME,  new IntegerSerializer(), new JsonSerializer());

        testDriver.pipeInput(factory.create(1, objectMapper.valueToTree(new DepositEvent(1, 100))));
        testDriver.pipeInput(factory.create(2, objectMapper.valueToTree(new WithdrawEvent(2, 100))));
        testDriver.pipeInput(factory.create(1, objectMapper.valueToTree(new WithdrawEvent(1, 200))));
        testDriver.pipeInput(factory.create(2, objectMapper.valueToTree(new DepositEvent(2, 100))));

        int amount = store.get(1);
        Assert.assertEquals(-100,amount);

        amount = store.get(2);
        Assert.assertEquals(0,amount);

    }


}
