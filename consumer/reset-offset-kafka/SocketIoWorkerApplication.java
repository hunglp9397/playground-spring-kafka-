@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
@EnableAsync
@EnableScheduling
public class SocketIoWorkerApplication implements CommandLineRunner {

    @Autowired
    AerospikeMessageService aerospikeMessageService;

    @Autowired
    AerospikeClient aerospikeClient;

    public static void main(String[] args) {
        SpringApplication.run(SocketIoWorkerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

    }

    public static void resetOffsetByTimestamp(
        String bootstrapServers,
        String topic,
        String groupId,
        long timestamp
    ) throws Exception {

        // 1. Consumer tạm để lấy offset theo time
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "temp-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (PartitionInfo p : partitions) {
            timestampsToSearch.put(
                new TopicPartition(topic, p.partition()),
                timestamp
            );
        }

        Map<TopicPartition, OffsetAndTimestamp> offsets =
            consumer.offsetsForTimes(timestampsToSearch);

        consumer.close();

        // 2. Map offset mới
        Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> e : offsets.entrySet()) {
            if (e.getValue() != null) {
                newOffsets.put(
                    e.getKey(),
                    new OffsetAndMetadata(e.getValue().offset())
                );
            }
        }

        // 3. Set offset cho group
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get();
        }

        System.out.println("✅ Reset offset OK");
    }
}