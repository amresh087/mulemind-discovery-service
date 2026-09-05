package com.mulemind.discovery.service;

import java.util.ArrayList;
import java.util.List;

import com.mulemind.discovery.dto.ApiEndpoint;
import com.mulemind.discovery.dto.ApplicationDetails;
import com.mulemind.discovery.dto.ConnectorDetails;
import com.mulemind.discovery.dto.DependencyDetails;
import com.mulemind.discovery.dto.FlowDetail;
import com.mulemind.discovery.dto.FlowReference;
import com.mulemind.discovery.dto.IntegrationDetails;
import com.mulemind.discovery.dto.RuntimeInfo;
import com.mulemind.discovery.dto.SourceFileDetails;
import com.mulemind.discovery.dto.TransformationDetail;
import com.mulemind.discovery.dto.TypeMetadata;
import com.mulemind.discovery.dto.VariableDetail;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectArtifactAnalysis {

    private ApplicationDetails application;

    @Builder.Default
    private List<ConnectorDetails> connectorDetails = new ArrayList<>();

    @Builder.Default
    private List<FlowReference> flowReferenceDetails = new ArrayList<>();

    @Builder.Default
    private IntegrationDetails integrations = IntegrationDetails.builder().build();

    @Builder.Default
    private DependencyDetails dependencyDetails = DependencyDetails.builder().build();

    @Builder.Default
    private List<String> sourceFiles = new ArrayList<>();

    @Builder.Default
    private List<SourceFileDetails> sourceFileDetails = new ArrayList<>();

    private RuntimeInfo runtimeInfo;
    private TypeMetadata typeMetadata;

    @Builder.Default
    private List<String> apis = new ArrayList<>();

    @Builder.Default
    private List<ApiEndpoint> apiDetails = new ArrayList<>();

    @Builder.Default
    private List<FlowDetail> flowDetails = new ArrayList<>();

    @Builder.Default
    private List<VariableDetail> variableDetails = new ArrayList<>();

    @Builder.Default
    private List<TransformationDetail> transformationDetails = new ArrayList<>();

    @Builder.Default
    private List<String> requestFields = new ArrayList<>();

    @Builder.Default
    private List<String> contractRequestFields = new ArrayList<>();

    @Builder.Default
    private List<String> explicitDataWeaveRequestFields = new ArrayList<>();

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

    @Builder.Default private List<String> muleVersions = new ArrayList<>();
    @Builder.Default private List<String> javaVersions = new ArrayList<>();
    @Builder.Default private List<String> dependencies = new ArrayList<>();
    @Builder.Default private List<String> mulePlugins = new ArrayList<>();
    @Builder.Default private List<String> connectors = new ArrayList<>();
    @Builder.Default private List<String> applicationNames = new ArrayList<>();
    @Builder.Default private List<String> muleRuntimes = new ArrayList<>();
    @Builder.Default private List<String> artifactProperties = new ArrayList<>();
    @Builder.Default private List<String> muleFlows = new ArrayList<>();
    @Builder.Default private List<String> httpListeners = new ArrayList<>();
    @Builder.Default private List<String> flowReferences = new ArrayList<>();
    @Builder.Default private List<String> transformations = new ArrayList<>();
    @Builder.Default private List<String> choices = new ArrayList<>();
    @Builder.Default private List<String> errorHandlers = new ArrayList<>();
    @Builder.Default private List<String> variables = new ArrayList<>();
    @Builder.Default private List<String> subflows = new ArrayList<>();
    @Builder.Default private List<String> applicationTypes = new ArrayList<>();
    @Builder.Default private List<String> globalConfigurations = new ArrayList<>();
}
