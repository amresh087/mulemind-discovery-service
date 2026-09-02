package com.mulemind.discovery.service;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectArtifactAnalysis {

    @Builder.Default
    private List<String> apis = new ArrayList<>();

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
