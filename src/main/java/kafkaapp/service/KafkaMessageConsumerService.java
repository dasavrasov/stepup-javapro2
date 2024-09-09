package kafkaapp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafkaapp.model.ConferenceRequest;
import kafkaapp.model.RegisterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KafkaMessageConsumerService {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumerService.class);
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Map<String, List<String>> registrations = new HashMap<>();
	private final Map<String, String> conferences = new HashMap<>();

	@SuppressWarnings({ "static-method", "unused" })
	@KafkaListener(topics = "registrations")
	public void onRegistrationMessage(@Payload String msg,
						  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
						  @Header(KafkaHeaders.OFFSET) Long offset) {
		LOG.info("Message Reg consumed {}", msg);
		try {
			RegisterRequest request = objectMapper.readValue(msg, RegisterRequest.class);
			registrations.computeIfAbsent(request.getConferenceID(), k -> new ArrayList<>()).add(request.getName());
		} catch (Exception e) {
			LOG.error("Error processing message {}", msg, e);
		}

	}
	@KafkaListener(topics = "conferences")
	public void onConferenceMessage(@Payload String msg,
						  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
						  @Header(KafkaHeaders.OFFSET) Long offset) {
		LOG.info("Message Conf consumed {}", msg);
		try {
			ConferenceRequest request = objectMapper.readValue(msg, ConferenceRequest.class);
			conferences.put(request.getConferenceID(), request.getName());
		} catch (Exception e) {
			LOG.error("Error processing message {}", msg, e);
		}

	}

	public Map<String, List<String>> getRegistrations() {
		return registrations;
	}

	public Map<String, String> getConferences() {
		return conferences;
	}
}