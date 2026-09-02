package com.mulemind.discovery.dto;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectScanResultEvent {

    private String eventType;
    private String eventVersion;

    private UUID documentId;
    private String documentName;
    private String tenant;
    private String objectName;
    private String status;

    @Builder.Default
    private List<ApiEndpoint> apis = new ArrayList<>();

    private ApplicationDetails application;

    @Builder.Default
    private List<ConnectorDetails> connectors = new ArrayList<>();

    @Builder.Default
    private List<FlowDetail> flows = new ArrayList<>();

    @Builder.Default
    private List<FlowReference> flowReferences = new ArrayList<>();

    @Builder.Default
    private List<VariableDetail> variables = new ArrayList<>();

    @Builder.Default
    private List<TransformationDetail> transformations = new ArrayList<>();

    private IntegrationDetails integrations;
    private DependencyDetails dependencies;

    @Builder.Default
    private List<SourceFileDetails> sourceFiles = new ArrayList<>();

    private RuntimeInfo runtimeInfo;
    private TypeMetadata typeMetadata;

    @Builder.Default
    private List<String> kafkaTopics = new ArrayList<>();

    @Builder.Default
    private List<String> mqEndpoints = new ArrayList<>();

    @Builder.Default
    private List<String> dbOperations = new ArrayList<>();

    @Builder.Default
    private List<String> fileOperations = new ArrayList<>();

    @JsonIgnore
    @Builder.Default
    private List<String> extractedFiles = new ArrayList<>();

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private LocalDateTime scannedAt = LocalDateTime.now();
}
