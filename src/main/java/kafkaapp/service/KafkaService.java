package kafkaapp.service;

import kafkaapp.config.KafkaConsumerConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

//    private final KafkaConsumerConfiguration kafkaConsumerConfiguration;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
//        this.kafkaConsumerConfiguration = kafkaConsumerConfiguration;
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

    public List<String> consume(String topicName) throws InterruptedException {
        List<String> messages = new ArrayList<>();
//        CountDownLatch latch = new CountDownLatch(1);
//
//        ConcurrentMessageListenerContainer<String, String> container = kafkaConsumerConfiguration.kafkaListenerContainerFactory().createContainer(topicName);
//        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
//            messages.add(record.value());
//            latch.countDown();
//        });
//
//        container.start();
//        latch.await(10, TimeUnit.SECONDS);
//        container.stop();
//
        return messages;
    }
}