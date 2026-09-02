package com.mulemind.discovery.service;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.mulemind.discovery.dto.DocumentKafkaEvent;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ProjectArchiveProcessingService {

    private static final Logger log = LoggerFactory.getLogger(ProjectArchiveProcessingService.class);

    private final MinioClient minioClient;
    private final ProjectArchiveProcessor projectArchiveProcessor;

    @Value("${minio.bucket-name:documents}")
    private String bucketName;

    public ProjectArtifactAnalysis processUploadedProject(DocumentKafkaEvent event) {
        if (event == null) {
            log.warn("Received null Kafka event for project processing");
            return new ProjectArtifactAnalysis();
        }

        if (!StringUtils.hasText(event.getObjectName())) {
            log.warn("Skipping project archive processing because object name is empty for event {}", event.getId());
            return new ProjectArtifactAnalysis();
        }

        try (InputStream objectStream = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(event.getObjectName())
                        .build())) {

            byte[] archiveBytes = objectStream.readAllBytes();
            ProjectArtifactAnalysis analysis = projectArchiveProcessor.parseArchive(archiveBytes);

            log.info("Processed archived project {} from bucket {}. APIs={}, KafkaTopics={}, MQ={}, DB={}, Files={}",
                    event.getObjectName(),
                    bucketName,
                    analysis.getApis().size(),
                    analysis.getKafkaTopics().size(),
                    analysis.getMqEndpoints().size(),
                    analysis.getDbOperations().size(),
                    analysis.getFileOperations().size());

            return analysis;
        } catch (Exception ex) {
            log.error("Failed to download and process project archive {} from MinIO bucket {}", event.getObjectName(), bucketName, ex);
            throw new IllegalStateException("Unable to process project archive from MinIO", ex);
        }
    }
}
