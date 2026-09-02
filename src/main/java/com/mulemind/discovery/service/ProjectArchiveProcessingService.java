package com.mulemind.discovery.service;

import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

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

    @Value("${project.archive.storage-path}")
    private String archiveStoragePath;

    public ProjectArtifactAnalysis processUploadedProject(DocumentKafkaEvent event) {
        if (event == null) {
            log.warn("Received null Kafka event for project processing");
            return new ProjectArtifactAnalysis();
        }

        if (!StringUtils.hasText(event.getObjectName())) {
            log.warn("Skipping project archive processing because object name is empty for event {}", event.getId());
            return new ProjectArtifactAnalysis();
        }

        Path workDirectory = null;
        try {
            Path storagePath = Path.of(archiveStoragePath);
            Files.createDirectories(storagePath);
            workDirectory = Files.createTempDirectory(storagePath, "mulemind-project-");
            String archiveFileName = StringUtils.hasText(event.getName())? Path.of(event.getName()).getFileName().toString() : "project.zip";
            Path archivePath = workDirectory.resolve(archiveFileName);
            Path extractionDirectory = workDirectory;

            try (InputStream objectStream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(bucketName).object(event.getObjectName()).build())) {
                Files.copy(objectStream, archivePath);
            }

            byte[] archiveBytes = Files.readAllBytes(archivePath);
            projectArchiveProcessor.extractArchive(archiveBytes, extractionDirectory);
            return projectArchiveProcessor.parseArchive(archiveBytes);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to process project archive from MinIO", ex);
        } finally {
           // deleteDirectory(workDirectory);
        }
    }

    private void deleteDirectory(Path directory) {
        if (directory == null) {
            return;
        }

        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ex) {
                            log.warn("Unable to delete temporary project archive path {}", path, ex);
                        }
                    });
        } catch (IOException ex) {
            log.warn("Unable to clean up temporary project archive directory {}", directory, ex);
        }
    }
}
