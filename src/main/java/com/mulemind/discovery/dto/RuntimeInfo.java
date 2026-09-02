package com.mulemind.discovery.dto;

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
public class RuntimeInfo {
    private String minMuleVersion;
    @Builder.Default
    private List<String> javaSpecificationVersions = new ArrayList<>();
}