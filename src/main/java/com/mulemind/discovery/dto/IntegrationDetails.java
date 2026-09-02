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
public class IntegrationDetails {
    @Builder.Default private List<String> kafka = new ArrayList<>();
    @Builder.Default private List<String> mq = new ArrayList<>();
    @Builder.Default private List<String> database = new ArrayList<>();
    @Builder.Default private List<String> file = new ArrayList<>();
    @Builder.Default private List<String> externalHttp = new ArrayList<>();
}