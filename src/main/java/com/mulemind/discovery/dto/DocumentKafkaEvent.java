package com.mulemind.discovery.dto;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentKafkaEvent {
    private UUID id;
    private String name;
    private String type;
    private String tenant;
    private String transactionTypeCode;
    private String version;
    private String status;
    private String contentType;
    private String objectName;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
