package com.mulemind.discovery.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;



@Service
public class DiscoveryKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(DiscoveryKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic.mulemind-scan-event}")
    private String scanEventTopic;

    public DiscoveryKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Object event, String key) {
        if (event == null) {
            log.warn("Skipping Kafka send because payload is null");
            return;
        }

        Message<Object> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, scanEventTopic)
                .setHeader(KafkaHeaders.KEY, key)
               // .setHeader("event_type", event.getEventType())
                //.setHeader("tenant", event.getTenant())
                .build();

        kafkaTemplate.send(message);
        
    }
}
