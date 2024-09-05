package kafkaapp.controller;

import kafkaapp.model.ConferenceRequest;
import kafkaapp.model.NewMembersResponse;
import kafkaapp.model.RegisterRequest;
import kafkaapp.service.KafkaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class ConfController {

    private final KafkaService kafkaService;

    private final Map<String, Set<String>> returnedMembers = new HashMap<>();

    public ConfController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PutMapping("/addConference")
    public void addConference(@RequestBody ConferenceRequest request) throws Exception {
        String topicName = request.getConferenceID();

        if (!kafkaService.topicExists(topicName)) {
            kafkaService.createTopic(topicName);
        }

        kafkaService.sendMessage(topicName, request.getName());
    }

    @PutMapping("/register")
    public ResponseEntity<String> register(@RequestBody RegisterRequest request) throws Exception {
        String topicName = request.getConferenceID();

        if (kafkaService.topicExists(topicName)) {
            kafkaService.sendMessage(topicName, request.getName());
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.status(400).body("Conference not found");
        }
    }

    @GetMapping("/getNewRegisters/{conferenceID}")
    public NewMembersResponse getNewRegisters(@PathVariable String conferenceID) throws InterruptedException {
        List<String> members = kafkaService.consume(conferenceID);

        NewMembersResponse response = new NewMembersResponse();
        response.setNames(members);
        return response;
    }

}