package com.mulemind.discovery.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiEndpoint {

    private String type;
    private String method;
    private boolean methodRestricted;
    private String path;
    private String listenerConfig;
    private String host;
    private Integer port;
    private String flow;

    @Builder.Default
    private java.util.List<String> requestFields = new java.util.ArrayList<>();
}