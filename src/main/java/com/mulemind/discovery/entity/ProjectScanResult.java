package com.mulemind.discovery.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "project_scan_result")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectScanResult {

    @Id
    @Column(name = "document_id", nullable = false)
    private UUID documentId;

    @Column(name = "document_name")
    private String documentName;

    @Column(name = "tenant")
    private String tenant;

    @Column(name = "object_name")
    private String objectName;

    @Column(name = "status")
    private String status;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_apis", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "api_path")
    @Builder.Default
    private List<String> apis = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_kafka_topics", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "topic_name")
    @Builder.Default
    private List<String> kafkaTopics = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_mq_endpoints", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "mq_endpoint")
    @Builder.Default
    private List<String> mqEndpoints = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_db_operations", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "db_operation")
    @Builder.Default
    private List<String> dbOperations = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_file_operations", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "file_operation")
    @Builder.Default
    private List<String> fileOperations = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "project_scan_extracted_files", joinColumns = @JoinColumn(name = "document_id"))
    @Column(name = "file_name")
    @Builder.Default
    private List<String> extractedFiles = new ArrayList<>();

    @Column(name = "scanned_at")
    @Builder.Default
    private LocalDateTime scannedAt = LocalDateTime.now();
}
