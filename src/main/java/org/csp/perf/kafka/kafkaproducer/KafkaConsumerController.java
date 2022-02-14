package org.csp.perf.kafka.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin
@RestController
@RequestMapping("/kafka/consumer")
@Slf4j
public class KafkaConsumerController {

    @Autowired
    KafkaConsumerService consumerService;

    @PostMapping("/read")
    public ResponseEntity<HttpStatus> readMessage(@RequestBody MessageRequest messageRequest) {
        consumerService.consumeMessage(messageRequest);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
