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
}
