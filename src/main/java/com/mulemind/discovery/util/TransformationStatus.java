package com.mulemind.discovery.util;

public enum TransformationStatus {
    CREATED("ZIP file received"),
    UPLOADED("File uploaded to the system"),
    SCANNING("Scanning the ZIP file contents"),
    SCAN_COMPLETED("Scanning process finished"),
    METADATA_PROCESSING("Extracting metadata from the files"),
    METADATA_COMPLETED("Metadata extraction completed"),
    AI_ANALYZING("Running AI analysis on the code"),
    AI_ANALYSIS_COMPLETED("AI analysis finished"),
    DOCUMENT_GENERATING("Generating documentation from analysis"),
    DOCUMENT_COMPLETED("Documentation generation completed"),
    COMPLETED("Workflow completed successfully"),
    FAILED("The workflow ended with an error");

    private final String description;

    TransformationStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}