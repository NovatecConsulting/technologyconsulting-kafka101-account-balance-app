package kafka101.kafka.configuration;

public interface KafkaConfiguration {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String TOPIC_NAME="user-transaction";

    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static String CONSUMER_CLIENT_ID="consumer1";
    public static String PRODUCER_CLIENT_ID="producer1";
    public static String STREAM_PROCESSOR_CLIENT_ID="stream-processor";


    public static Integer MAX_POLL_RECORDS=100;
    public static String OFFSET_RESET_EARLIER="earliest";
    public static String STREAM_STATE_STORE="balance-store";
    public static String STREAM_STATE_DIR="stream1";
}
