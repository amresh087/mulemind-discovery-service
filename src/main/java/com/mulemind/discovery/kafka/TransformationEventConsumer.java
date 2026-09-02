package com.mulemind.discovery.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.mulemind.discovery.dto.DocumentKafkaEvent;

@Component
public class TransformationEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransformationEventConsumer.class);

    
    @KafkaListener(topics = "${app.kafka.topic.mulemind-upload-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void onProjectUploaded(DocumentKafkaEvent event) {
        handleEvent(event, "mulemind-upload-events");
    }

    private void handleEvent(DocumentKafkaEvent event, String topic) {
        if (event == null) {
            log.warn("Received null Kafka event from topic {}", topic);
            return;
        }

        log.info("Received Kafka event from topic {}: {}", topic, event);
        // transactionService.processTransformationEvent(event);
    }
}
