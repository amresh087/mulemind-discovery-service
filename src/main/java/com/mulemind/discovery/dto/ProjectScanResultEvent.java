package com.mulemind.discovery.dto;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectScanResultEvent {

    private UUID documentId;
    private String documentName;
    private String tenant;
    private String objectName;
    private String status;

    @Builder.Default
    private List<ApiEndpoint> apis = new ArrayList<>();

    @Builder.Default
    private List<FlowDetail> flows = new ArrayList<>();

    @Builder.Default
    private List<VariableDetail> variables = new ArrayList<>();

    @Builder.Default
    private List<TransformationDetail> transformations = new ArrayList<>();

    @Builder.Default
    private List<String> kafkaTopics = new ArrayList<>();

    @Builder.Default
    private List<String> mqEndpoints = new ArrayList<>();

    @Builder.Default
    private List<String> dbOperations = new ArrayList<>();

    @Builder.Default
    private List<String> fileOperations = new ArrayList<>();

    @Builder.Default
    private List<String> extractedFiles = new ArrayList<>();

    @Builder.Default
    private LocalDateTime scannedAt = LocalDateTime.now();
}
