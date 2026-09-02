package com.mulemind.discovery.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;

class ProjectArchiveProcessorTest {

    @Test
    void shouldParseArchiveContentsAndExtractArtifacts() throws IOException {
        byte[] zipBytes = buildSampleZip();

        ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
        ProjectArtifactAnalysis analysis = processor.parseArchive(zipBytes);

        assertNotNull(analysis);
        assertFalse(analysis.getApis().isEmpty());
        assertFalse(analysis.getKafkaTopics().isEmpty());
        assertFalse(analysis.getMqEndpoints().isEmpty());
        assertFalse(analysis.getDbOperations().isEmpty());
        assertFalse(analysis.getFileOperations().isEmpty());
    }

    @Test
    void shouldIgnoreCompiledClassFilesAndHiddenEntries() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
            zipOut.putNextEntry(new ZipEntry("project/api.yaml"));
            zipOut.write("/orders:\n  get:\n    responses:\n      '200':\n        body:\n          application/json:".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();

            zipOut.putNextEntry(new ZipEntry("project/target/classes/com/example/Test.class"));
            zipOut.write(new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, 0x00, 0x00, 0x00, 0x01});
            zipOut.closeEntry();

            zipOut.putNextEntry(new ZipEntry("project/.hidden/config.properties"));
            zipOut.write("kafka.topic=ignored.topic".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
        }

        ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
        ProjectArtifactAnalysis analysis = processor.parseArchive(bos.toByteArray());

        assertFalse(analysis.getExtractedFiles().contains("project/target/classes/com/example/Test.class"));
        assertFalse(analysis.getExtractedFiles().contains("project/.hidden/config.properties"));
        assertFalse(analysis.getApis().isEmpty());
        assertFalse(analysis.getApis().stream().noneMatch(path -> path.contains("/orders")));
    }

    @Test
    void shouldHandleStoredEntriesWithDataDescriptorFlag() throws IOException {
        byte[] zipBytes = buildZipWithStoredEntryDataDescriptor();

        ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
        ProjectArtifactAnalysis analysis = processor.parseArchive(zipBytes);

        assertNotNull(analysis);
        assertFalse(analysis.getExtractedFiles().isEmpty());
        assertFalse(analysis.getApis().isEmpty());
    }

    @Test
    void shouldScanMuleXmlFilesUnderSrcMainMule() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
            zipOut.putNextEntry(new ZipEntry("project/src/main/mule/xyz.xml"));
            zipOut.write("<mule><flow name=\"orders\"><http:listener path=\"/orders\"/></flow></mule>"
                    .getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();

                zipOut.putNextEntry(new ZipEntry("project/src/main/mule/subfolder/deeper.xml"));
                zipOut.write("<mule><flow name=\"customers\"/></mule>".getBytes(StandardCharsets.UTF_8));
                zipOut.closeEntry();
        }

        ProjectArtifactAnalysis analysis = new ProjectArchiveProcessor().parseArchive(bos.toByteArray());

        assertFalse(analysis.getExtractedFiles().stream()
                .noneMatch(fileName -> fileName.equals("project/src/main/mule/xyz.xml")));
            assertFalse(analysis.getExtractedFiles().stream()
                .noneMatch(fileName -> fileName.equals("project/src/main/mule/subfolder/deeper.xml")));
    }

    @Test
    void shouldExtractFolderAndSubfolderStructure() throws IOException {
        byte[] zipBytes = buildNestedMuleZip();
        Path destination = Files.createTempDirectory("mulemind-extract-");

        new ProjectArchiveProcessor().extractArchive(zipBytes, destination);

        assertFalse(Files.notExists(destination.resolve("project/src/main/mule/xyz.xml")));
        assertFalse(Files.notExists(destination.resolve("project/src/main/mule/subfolder/deeper.xml")));
    }

    private byte[] buildNestedMuleZip() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
            zipOut.putNextEntry(new ZipEntry("project/src/main/mule/xyz.xml"));
            zipOut.write("<mule/>".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
            zipOut.putNextEntry(new ZipEntry("project/src/main/mule/subfolder/deeper.xml"));
            zipOut.write("<mule/>".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
        }
        return bos.toByteArray();
    }

    private byte[] buildSampleZip() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
            zipOut.putNextEntry(new ZipEntry("project/api.xml"));
            zipOut.write("<api><paths><path>/orders</path><path>/customers</path></paths></api>".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();

            zipOut.putNextEntry(new ZipEntry("project/transform.dwl"));
            zipOut.write("payload map { orderId: $.id }\nvar topic = 'orders.created'".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();

            zipOut.putNextEntry(new ZipEntry("project/contract.raml"));
            zipOut.write("#%RAML 1.0\n/title: Orders API\nversion: 1\n/orders:\n  get:\n    responses:\n      '200':\n        body:\n          application/json:".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();

            zipOut.putNextEntry(new ZipEntry("project/app.properties"));
            zipOut.write("mq.rabbitmq.host=localhost\nkafka.topic=orders.created\ndb.jdbc.url=jdbc:mysql://localhost:3306/app\nfile.path=/tmp/output".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
        }

        return bos.toByteArray();
    }

    private byte[] buildZipWithStoredEntryDataDescriptor() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
            zipOut.putNextEntry(new ZipEntry("project/api.xml"));
            zipOut.write("<api><paths><path>/orders</path></paths></api>".getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
        }

        byte[] zipBytes = bos.toByteArray();
        int headerOffset = 0;
        while (headerOffset < zipBytes.length - 4) {
            if (zipBytes[headerOffset] == 'P' && zipBytes[headerOffset + 1] == 'K' && zipBytes[headerOffset + 2] == 3 && zipBytes[headerOffset + 3] == 4) {
                break;
            }
            headerOffset++;
        }

        if (headerOffset < 0 || headerOffset + 30 > zipBytes.length) {
            throw new IllegalStateException("Could not locate ZIP local header");
        }

        int flag = ((zipBytes[headerOffset + 6] & 0xFF) | ((zipBytes[headerOffset + 7] & 0xFF) << 8));
        int patchedFlag = flag | 0x0008;
        zipBytes[headerOffset + 6] = (byte) (patchedFlag & 0xFF);
        zipBytes[headerOffset + 7] = (byte) ((patchedFlag >>> 8) & 0xFF);
        zipBytes[headerOffset + 8] = 0;
        zipBytes[headerOffset + 9] = 0;

        return zipBytes;
    }
}
