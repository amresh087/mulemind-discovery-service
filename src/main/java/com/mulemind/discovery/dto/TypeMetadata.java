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
public class TypeMetadata {
    private PayloadMetadata inputPayload;
    private PayloadMetadata outputPayload;
    private AttributeMetadata inputAttributes;
    private AttributeMetadata outputAttributes;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PayloadMetadata {
        private String name;
        private String format;
        private String type;
        private java.util.Map<String, String> schema;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AttributeMetadata {
        private String type;
        @Builder.Default
        private List<String> fields = new ArrayList<>();
    }
}