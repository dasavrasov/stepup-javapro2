package kafkaapp.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Service
public class KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;
    private final ConsumerFactory<String, String> consumerFactory;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin,ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
        this.consumerFactory = consumerFactory;
    }

    public void sendMessage(String topicName, String message) {
        kafkaTemplate.send(topicName, message);
    }

    public boolean topicExists(String topicName) throws Exception {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            return adminClient.listTopics().names().get().contains(topicName);
        }
    }

    public void createTopic(String topicName) throws Exception {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1))).all().get();
        }
    }

    public List<String> consume(String topicName) {
        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(topicName));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            List<String> messages = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                messages.add(record.value());
            }

            return messages;
        }
    }

}