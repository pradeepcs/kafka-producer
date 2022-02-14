package org.csp.perf.kafka.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Service
@EnableAsync
@Slf4j
public class PublishService {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Async
    public void inventoryPublish(MessageRequest messageRequest) {
        List<MessageRequest.Message> messages = messageRequest.messages;
        for(int i = 0; i < messageRequest.loopCount; i++) {
            messages.stream().forEach(message -> buildMessage(messageRequest, message));
        }
        log.info("publish completed");
    }

    private void buildMessage(MessageRequest messageRequest, MessageRequest.Message eachMessage) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(messageRequest.topicName, eachMessage.getKey(), eachMessage.payload);
        eachMessage.getHeaders().entrySet().stream().forEach(
                headerEntry -> producerRecord
                        .headers()
                        .add(headerEntry.getKey(), headerEntry.getValue().toString().getBytes()));
        kafkaTemplate.send(producerRecord);
    }

    private void sendMessage(Message<String> message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message {} due to {}", message.getPayload(), ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.debug("Sent message {} with offset {}", message.getPayload(), result.getRecordMetadata().offset());
            }
        });
    }

    public Message<String> sendKafkaMessage(String payload, String topic, String messageKey, String messageId) {
        Message<String> message = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, messageKey)
                .setHeader("MESSAGE_ID", messageId)
                .build();
        sendMessage(message);
        return message;
    }



}