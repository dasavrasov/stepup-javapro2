package kafkaapp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafkaapp.model.ConferenceRequest;
import kafkaapp.model.RegisterRequest;
import kafkaapp.service.KafkaMessageConsumerService;
import kafkaapp.service.KafkaMessageProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class ConfController {

    private final KafkaMessageProducerService producerService;

    private KafkaMessageConsumerService kafkaMessageConsumerService;

    private final ObjectMapper objectMapper;

    public ConfController(KafkaMessageProducerService producerService, KafkaMessageConsumerService kafkaMessageConsumerService, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.kafkaMessageConsumerService = kafkaMessageConsumerService;
        this.objectMapper = objectMapper;
    }

    @PutMapping("/register")
    public ResponseEntity<String> register(@RequestBody RegisterRequest request) {
        try {
            if (!kafkaMessageConsumerService.getConferences().containsKey(request.getConferenceID())) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Conference with id "+request.getConferenceID()+" not found");
            }
            String requestJson = objectMapper.writeValueAsString(request);
            producerService.send("registrations", requestJson);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error serializing request");
        }
    }
    @PutMapping("/addConference")
    public ResponseEntity<String>  addConference(@RequestBody ConferenceRequest request) {
        try {
            String requestJson = objectMapper.writeValueAsString(request);
            producerService.send("conferences", requestJson);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error serializing request");
        }
    }

    @GetMapping("/getNewRegisters/{conferenceID}")
    public ResponseEntity<List<String>> getNewRegisters(@PathVariable String conferenceID) {
        Map<String, List<String>> registrations = kafkaMessageConsumerService.getRegistrations();
        List<String> names = registrations.get(conferenceID);
        if (names != null) {
            return ResponseEntity.ok(names);
        } else {
            return ResponseEntity.notFound().build();
        }
    }


}