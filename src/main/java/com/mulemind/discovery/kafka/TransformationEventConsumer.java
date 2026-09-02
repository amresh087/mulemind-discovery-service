package com.mulemind.discovery.kafka;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.mulemind.discovery.dto.DocumentKafkaEvent;
import com.mulemind.discovery.dto.ProjectScanResultEvent;
import com.mulemind.discovery.service.ProjectArchiveProcessingService;
import com.mulemind.discovery.service.ProjectArtifactAnalysis;
import com.mulemind.discovery.service.ProjectScanResultService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class TransformationEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransformationEventConsumer.class);

    private final ProjectArchiveProcessingService projectArchiveProcessingService;
    private final ProjectScanResultService projectScanResultService;
    private final DiscoveryKafkaProducer discoveryKafkaProducer;

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

        try {
            ProjectArtifactAnalysis analysis = projectArchiveProcessingService.processUploadedProject(event);

            ProjectScanResultEvent scanResult = ProjectScanResultEvent.builder()
                    .documentId(event.getId())
                    .eventType("MULE_APPLICATION_SCANNED")
                    .eventVersion("1.0")
                    .documentName(event.getName())
                    .tenant(event.getTenant())
                    .objectName(event.getObjectName())
                    .status("SCANNED")
                    .apis(List.copyOf(analysis.getApiDetails()))
                    .application(analysis.getApplication())
                    .connectors(List.copyOf(analysis.getConnectorDetails()))
                    .flows(List.copyOf(analysis.getFlowDetails()))
                    .flowReferences(List.copyOf(analysis.getFlowReferenceDetails()))
                    .variables(List.copyOf(analysis.getVariableDetails()))
                    .transformations(List.copyOf(analysis.getTransformationDetails()))
                        .integrations(com.mulemind.discovery.dto.IntegrationDetails.builder()
                            .kafka(List.copyOf(analysis.getKafkaTopics()))
                            .mq(List.copyOf(analysis.getMqEndpoints()))
                            .database(List.copyOf(analysis.getDbOperations()))
                            .file(List.copyOf(analysis.getFileOperations()))
                            .externalHttp(List.of())
                            .build())
                    .dependencies(analysis.getDependencyDetails())
                    .sourceFiles(List.copyOf(analysis.getSourceFiles()))
                    .kafkaTopics(List.copyOf(analysis.getKafkaTopics()))
                    .mqEndpoints(List.copyOf(analysis.getMqEndpoints()))
                    .dbOperations(List.copyOf(analysis.getDbOperations()))
                    .fileOperations(List.copyOf(analysis.getFileOperations()))
                    .extractedFiles(List.copyOf(analysis.getExtractedFiles()))
                    .build();

            projectScanResultService.save(scanResult);
            discoveryKafkaProducer.send(scanResult, event.getId() != null ? event.getId().toString() : event.getName());

            log.info("Archive inspection completed for object {}. APIs={}, KafkaTopics={}, MQ={}, DB={}, Files={}",
                    event.getObjectName(),
                    analysis.getApis(),
                    analysis.getKafkaTopics(),
                    analysis.getMqEndpoints(),
                    analysis.getDbOperations(),
                    analysis.getFileOperations());
        } catch (Exception ex) {
            log.error("Archive processing failed for document {} object {} in topic {}",
                    event.getId(), event.getObjectName(), topic, ex);
        }
    }
}
