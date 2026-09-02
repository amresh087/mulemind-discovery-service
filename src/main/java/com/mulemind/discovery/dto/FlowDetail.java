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
public class FlowDetail {

    private String name;
    private String type;
    private String trigger;

    @Builder.Default
    private List<String> processors = new ArrayList<>();

    @Builder.Default
    private List<String> references = new ArrayList<>();
}