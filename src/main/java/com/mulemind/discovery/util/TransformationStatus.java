package com.mulemind.discovery.util;

public enum TransformationStatus {
    UPLOADED("File uploaded to the system"),
    SCANNING("Scanning the ZIP file contents"),
    METADATA_PROCESSING("Extracting metadata from the files"),
    AI_ANALYZING("Running AI analysis on the code"),
    DOCUMENT_GENERATING("Generating documentation from analysis"),
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