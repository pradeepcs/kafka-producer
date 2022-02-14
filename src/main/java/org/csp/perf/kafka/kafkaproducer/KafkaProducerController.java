package org.csp.perf.kafka.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin
@RestController
@RequestMapping("/kafka/producer/load")
@Slf4j
public class KafkaProducerController {

    @Autowired
    PublishService publishService;

    @PostMapping("/{loadType}")
    public ResponseEntity<HttpStatus> publishMessage(@RequestBody MessageRequest messageRequest) {
        publishService.inventoryPublish(messageRequest);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
