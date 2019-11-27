package kafka101.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka101.events.DepositEvent;
import kafka101.events.WithdrawEvent;
import kafka101.kafka.configuration.KafkaConfiguration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AccountService {

    @Autowired
    Producer<Integer, JsonNode> producer;

    @Autowired
    Consumer<Integer, JsonNode> consumer;

    @Autowired
    KafkaStreams streams;


    ObjectMapper objectMapper = new ObjectMapper();


    private boolean withdrawValidation(int id, int amount){
        ReadOnlyKeyValueStore<Integer, Integer> keyValueStore =
                streams.store(KafkaConfiguration.STREAM_STATE_STORE, QueryableStoreTypes.keyValueStore());
        Integer currentAmount = keyValueStore.get(id);
        if (currentAmount==null ||(currentAmount-amount)<0){
            return false;
        }
        else{
            return true;
        }
    }


    public String depositService(int id, int amount){
        DepositEvent event = new DepositEvent(id,amount);
        JsonNode jsonNode = objectMapper.valueToTree(event);
        ProducerRecord<Integer, JsonNode> record = new ProducerRecord<Integer, JsonNode>(KafkaConfiguration.TOPIC_NAME, event.getUserID(),jsonNode);
        producer.send(record);
        return event.toString();
    }

    public String withdrawService(int id, int amount){
        if(withdrawValidation(id,amount)){
            WithdrawEvent event = new WithdrawEvent(id,amount);
            JsonNode jsonNode = objectMapper.valueToTree(event);
            ProducerRecord<Integer, JsonNode> record = new ProducerRecord<Integer, JsonNode>(KafkaConfiguration.TOPIC_NAME, event.getUserID(),jsonNode);
            producer.send(record);
            return event.toString();
        }
        else{
            return "Withdraw request is invalid";
        }
    }

    public String getTransaction(){
        StringBuilder builder = new StringBuilder("History of transactions");
        builder.append("<br/>");
        JsonNode jsonNode = null;
        ConsumerRecords<Integer, JsonNode> consumerRecords = consumer.poll(1000);
        for (ConsumerRecord<Integer, JsonNode> record : consumerRecords) {
            jsonNode = record.value();
            String event = "User ID: " + jsonNode.get("userID") + " type of transaction: " + jsonNode.get("eventType") + " amount: " + jsonNode.get("amount");
            builder.append(event + "<br/>");
        }
        if (jsonNode == null){
            builder.append("No transaction has been made.");
        }
        return builder.toString();
    }

    public String getBalance(int id){
        ReadOnlyKeyValueStore<Integer, Integer> keyValueStore =
            streams.store(KafkaConfiguration.STREAM_STATE_STORE, QueryableStoreTypes.keyValueStore());
        Integer amount = keyValueStore.get(id);
        if (amount == null){
            return "No customer with id: " + id;
        }else{
            return "Customer: " + id + " Account balance: " + amount;
        }
    }

    public String getAllBalances(){
        StringBuilder builder = new StringBuilder("Account balance");
        builder.append("<br/>");
        ReadOnlyKeyValueStore<Integer, Integer> keyValueStore =
                streams.store(KafkaConfiguration.STREAM_STATE_STORE, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Integer, Integer> range = keyValueStore.all();
        while (range.hasNext()) {
            KeyValue<Integer, Integer> next = range.next();
            builder.append("Customer: " + next.key + " Account balance: " + next.value );
            builder.append("<br/>");
        }
        return builder.toString();
    }
}
