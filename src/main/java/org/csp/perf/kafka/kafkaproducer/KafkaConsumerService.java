package org.csp.perf.kafka.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.UUID;

@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    ConsumerFactory consumerFactory;

    @Autowired
    PublishService publishService;

    public void consumeMessage(MessageRequest messageRequest) {
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerFactory.createConsumer();
        TopicPartition tp = new TopicPartition(messageRequest.topicName, 0);
        kafkaConsumer.assign(Arrays.asList(tp));
        kafkaConsumer.seek(tp, messageRequest.startOffset);
        boolean run = true;
        while (run) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(
                    Duration.of(messageRequest.pollDuration, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> record : records) {
                log.info("republishing message for offset {}", record.offset());
                publishService.sendKafkaMessage(record.value(), messageRequest.publishTopicName, record.key(),
                        UUID.randomUUID().toString());
                if (record.offset() == messageRequest.endOffset) {
                    run = false;
                    break;
                }
            }
        }
    }
}
