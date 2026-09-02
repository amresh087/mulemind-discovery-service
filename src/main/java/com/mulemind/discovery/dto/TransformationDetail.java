package com.mulemind.discovery.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransformationDetail {

    private String type;
    private String flow;
    private String outputMimeType;
    private String script;
}