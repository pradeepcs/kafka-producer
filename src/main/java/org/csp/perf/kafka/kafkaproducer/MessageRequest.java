package org.csp.perf.kafka.kafkaproducer;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Setter
@Getter
public class MessageRequest {
    String topicName;
    long startOffset;
    long endOffset;
    long pollDuration;
    String publishTopicName;
    Integer loopCount;
    List<Message> messages;

    @Setter
    @Getter
    public static class Message {
        Map<String, Object> headers;
        String key;
        String payload;
    }
}
