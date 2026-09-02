package com.mulemind.discovery.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.mulemind.discovery.dto.ProjectScanResultEvent;
import com.mulemind.discovery.entity.ProjectScanResult;
import com.mulemind.discovery.repository.ProjectScanResultRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ProjectScanResultService {

    private final ProjectScanResultRepository projectScanResultRepository;

    public ProjectScanResult save(ProjectScanResultEvent event) {
        if (event == null) {
            return null;
        }

        ProjectScanResult entity = ProjectScanResult.builder()
                .documentId(event.getDocumentId())
                .documentName(event.getDocumentName())
                .tenant(event.getTenant())
                .objectName(event.getObjectName())
                .status(event.getStatus())
                .apis(event.getApis() == null ? List.of() : event.getApis().stream()
                    .map(api -> api == null ? null : api.getPath())
                    .filter(path -> path != null && !path.isBlank())
                    .toList())
                .kafkaTopics(normalizeList(event.getKafkaTopics()))
                .mqEndpoints(normalizeList(event.getMqEndpoints()))
                .dbOperations(normalizeList(event.getDbOperations()))
                .fileOperations(normalizeList(event.getFileOperations()))
                .extractedFiles(normalizeList(event.getExtractedFiles()))
                .scannedAt(event.getScannedAt())
                .build();

        return projectScanResultRepository.save(entity);
    }

    private List<String> normalizeList(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream().filter(value -> value != null && !value.isBlank()).toList();
    }
}
