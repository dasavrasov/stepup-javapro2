package kafkaapp;

import kafkaapp.service.KafkaMessageConsumerService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;

@SpringBootTest
public class KafkaConsumerServiceTest {

	@Autowired
	private KafkaMessageConsumerService kafkaMessageConsumerService;

	private Consumer<String, String> consumer;

	@BeforeEach
	public void setUp() {
	}

	@Test
	public void consumeMessages() {
		kafkaMessageConsumerService.onRegistrationMessage("test message", "registrations", 0, 0L);
		kafkaMessageConsumerService.onConferenceMessage("test message", "conferences", 0, 0L);
	}
	@AfterEach
	public void tearDown() {
	}
}
