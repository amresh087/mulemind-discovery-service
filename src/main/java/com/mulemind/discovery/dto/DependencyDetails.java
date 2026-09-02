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
public class DependencyDetails {
    @Builder.Default private List<String> http = new ArrayList<>();
    @Builder.Default private List<String> sockets = new ArrayList<>();
}