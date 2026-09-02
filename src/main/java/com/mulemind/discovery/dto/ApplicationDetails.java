package com.mulemind.discovery.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationDetails {
    private String name;
    private String groupId;
    private String artifactId;
    private String version;
    private String packaging;
    private String muleRuntime;
    private String muleMavenPluginVersion;
}