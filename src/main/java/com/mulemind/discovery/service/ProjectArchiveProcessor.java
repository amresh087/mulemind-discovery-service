package com.mulemind.discovery.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.stereotype.Component;

@Component
public class ProjectArchiveProcessor {

    private static final Set<String> XML_EXTENSIONS = Set.of(".xml", ".xsd", ".wsdl");
    private static final Set<String> DWL_EXTENSIONS = Set.of(".dwl", ".dwl.xml");
    private static final Set<String> RAML_EXTENSIONS = Set.of(".raml", ".yaml", ".yml");
    private static final Set<String> PROPERTY_EXTENSIONS = Set.of(".properties", ".env", ".cfg");

    private static final Pattern API_PATTERN = Pattern.compile("(?:<path>|<uri>|@Path\\(|/[-A-Za-z0-9_./{}]+)");
    private static final Pattern KAFKA_PATTERN = Pattern.compile("(?:kafka|topic|topicName|topic-name).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9._-]+)[\"']?", Pattern.CASE_INSENSITIVE);
    private static final Pattern MQ_PATTERN = Pattern.compile("(?:mq|rabbitmq|amqp|jms|activemq|queue|exchange|destination).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9._:/-]+)[\"']?", Pattern.CASE_INSENSITIVE);
    private static final Pattern DB_PATTERN = Pattern.compile("(?:jdbc:[A-Za-z0-9._:/+-]+|\\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE TABLE|ALTER TABLE|DROP TABLE|MERGE)\\b.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FILE_PATTERN = Pattern.compile("(?:file|path|directory|folder|input|output|location).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9_./\\:-]+)[\"']?", Pattern.CASE_INSENSITIVE);

    public ProjectArtifactAnalysis parseArchive(byte[] archiveBytes) {
        ProjectArtifactAnalysis analysis = new ProjectArtifactAnalysis();
        if (archiveBytes == null || archiveBytes.length == 0) {
            return analysis;
        }

        byte[] zipBytes = sanitizeMalformedStoredEntries(archiveBytes);

        try (ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }

                String fileName = entry.getName();
                if (shouldSkipEntry(fileName)) {
                    continue;
                }

                System.out.println("Processing entry: ======================== " + fileName);

                String content = readEntryContent(zipInputStream);

                if (!content.isBlank()) {
                    analysis.getExtractedFiles().add(fileName);
                    analyzeContent(fileName, content, analysis);
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to parse project archive", ex);
        }

        return analysis;
    }

    private static byte[] sanitizeMalformedStoredEntries(byte[] archiveBytes) {
        if (archiveBytes == null || archiveBytes.length < 30) {
            return archiveBytes;
        }

        byte[] sanitized = archiveBytes.clone();
        int offset = 0;

        while (offset <= sanitized.length - 30) {
            if (sanitized[offset] == 'P' && sanitized[offset + 1] == 'K' && sanitized[offset + 2] == 0x03 && sanitized[offset + 3] == 0x04) {
                int generalPurposeBitFlag = readUnsignedShort(sanitized, offset + 6);
                int compressionMethod = readUnsignedShort(sanitized, offset + 8);

                if (compressionMethod == 0 && (generalPurposeBitFlag & 0x0008) != 0) {
                    int repairedFlag = generalPurposeBitFlag & ~0x0008;
                    sanitized[offset + 6] = (byte) (repairedFlag & 0xFF);
                    sanitized[offset + 7] = (byte) ((repairedFlag >>> 8) & 0xFF);
                }

                int fileNameLength = readUnsignedShort(sanitized, offset + 26);
                int extraFieldLength = readUnsignedShort(sanitized, offset + 28);
                int compressedSize = readInt(sanitized, offset + 18);
                int nextHeaderOffset = offset + 30 + fileNameLength + extraFieldLength + compressedSize;

                if (nextHeaderOffset <= sanitized.length) {
                    offset = nextHeaderOffset;
                    continue;
                }
            }
            offset++;
        }

        return sanitized;
    }

    private static int readUnsignedShort(byte[] data, int offset) {
        return (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8);
    }

    private static int readInt(byte[] data, int offset) {
        return (data[offset] & 0xFF)
                | ((data[offset + 1] & 0xFF) << 8)
                | ((data[offset + 2] & 0xFF) << 16)
                | ((data[offset + 3] & 0xFF) << 24);
    }

    private static boolean shouldSkipEntry(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return true;
        }

        String normalized = fileName.replace('\\', '/');
        String lowerName = normalized.toLowerCase(Locale.ROOT);

        if (lowerName.startsWith(".") || lowerName.contains("/.")) {
            return true;
        }

        String fileSegment = lowerName.substring(lowerName.lastIndexOf('/') + 1);
        if (fileSegment.startsWith(".")) {
            return true;
        }

        return lowerName.endsWith(".class");
    }

    private void analyzeContent(String fileName, String content, ProjectArtifactAnalysis analysis) {
        String normalizedName = fileName.toLowerCase(Locale.ROOT);

        if (isXmlFile(normalizedName) || normalizedName.contains("api") || normalizedName.contains("swagger") || normalizedName.contains("openapi")) {
            extractApiEndpoints(content, analysis);
        }

        if (isDwlFile(normalizedName) || normalizedName.contains("transform") || normalizedName.contains("dataweave")) {
            extractKafkaTopics(content, analysis);
            extractMqEndpoints(content, analysis);
            extractDbOperations(content, analysis);
            extractFileOperations(content, analysis);
        }

        if (isRamlOrYaml(normalizedName)) {
            extractApiEndpoints(content, analysis);
        }

        if (isPropertyFile(normalizedName)) {
            extractKafkaTopics(content, analysis);
            extractMqEndpoints(content, analysis);
            extractDbOperations(content, analysis);
            extractFileOperations(content, analysis);
        }

        if (normalizedName.contains("xml") || normalizedName.contains("wsdl") || normalizedName.contains("raml") || normalizedName.contains("api")) {
            extractDbOperations(content, analysis);
        }

        if (content.contains("topic") || content.contains("queue") || content.contains("mq") || content.contains("jdbc") || content.contains("file://") || content.contains("/tmp") || content.contains("/opt")) {
            extractKafkaTopics(content, analysis);
            extractMqEndpoints(content, analysis);
            extractDbOperations(content, analysis);
            extractFileOperations(content, analysis);
        }
    }

    private void extractApiEndpoints(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = API_PATTERN.matcher(content);
        while (matcher.find()) {
            String candidate = matcher.group().trim();
            if (candidate.startsWith("<") || candidate.startsWith("@")) {
                continue;
            }
            if (candidate.startsWith("/")) {
                addUnique(analysis.getApis(), candidate);
            }
        }

        Pattern ramlEndpointPattern = Pattern.compile("^(\\s*/[A-Za-z0-9_./{}-]+)", Pattern.MULTILINE);
        Matcher ramlMatcher = ramlEndpointPattern.matcher(content);
        while (ramlMatcher.find()) {
            addUnique(analysis.getApis(), ramlMatcher.group(1).trim());
        }
    }

    private void extractKafkaTopics(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = KAFKA_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                addUnique(analysis.getKafkaTopics(), value);
            }
        }

        Pattern directTopicPattern = Pattern.compile("['\"]([A-Za-z0-9._-]+)['\"]", Pattern.CASE_INSENSITIVE);
        Matcher directMatcher = directTopicPattern.matcher(content);
        while (directMatcher.find()) {
            String value = directMatcher.group(1);
            if (content.toLowerCase(Locale.ROOT).contains("topic") && (value.contains(".") || value.contains("-") || value.contains("_"))) {
                addUnique(analysis.getKafkaTopics(), value);
            }
        }
    }

    private void extractMqEndpoints(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = MQ_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                addUnique(analysis.getMqEndpoints(), value);
            }
        }
    }

    private void extractDbOperations(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = DB_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group().trim();
            if (!value.isBlank()) {
                addUnique(analysis.getDbOperations(), value);
            }
        }

        Pattern jdbcPattern = Pattern.compile("jdbc:[A-Za-z0-9._:/+-]+", Pattern.CASE_INSENSITIVE);
        Matcher jdbcMatcher = jdbcPattern.matcher(content);
        while (jdbcMatcher.find()) {
            addUnique(analysis.getDbOperations(), jdbcMatcher.group());
        }
    }

    private void extractFileOperations(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = FILE_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                addUnique(analysis.getFileOperations(), value);
            }
        }

        if (content.contains("file://") || content.contains("/tmp") || content.contains("/opt") || content.contains("/var/")) {
            Pattern pathPattern = Pattern.compile("(?:file://|/[/A-Za-z0-9._-]+)");
            Matcher pathMatcher = pathPattern.matcher(content);
            while (pathMatcher.find()) {
                String value = pathMatcher.group().trim();
                if (!value.isBlank()) {
                    addUnique(analysis.getFileOperations(), value);
                }
            }
        }
    }

    private static boolean isXmlFile(String normalizedName) {
        return XML_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    private static boolean isDwlFile(String normalizedName) {
        return DWL_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    private static boolean isRamlOrYaml(String normalizedName) {
        return RAML_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    private static boolean isPropertyFile(String normalizedName) {
        return PROPERTY_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    private static String readEntryContent(InputStream inputStream) throws IOException {
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void addUnique(List<String> values, String value) {
        if (value == null || value.isBlank()) {
            return;
        }

        String normalized = value.trim();
        if (values.stream().noneMatch(existing -> existing.equalsIgnoreCase(normalized))) {
            values.add(normalized);
        }
    }

    private static void merge(ProjectArtifactAnalysis target, ProjectArtifactAnalysis source) {
        source.getApis().forEach(value -> addUnique(target.getApis(), value));
        source.getKafkaTopics().forEach(value -> addUnique(target.getKafkaTopics(), value));
        source.getMqEndpoints().forEach(value -> addUnique(target.getMqEndpoints(), value));
        source.getDbOperations().forEach(value -> addUnique(target.getDbOperations(), value));
        source.getFileOperations().forEach(value -> addUnique(target.getFileOperations(), value));
        source.getExtractedFiles().forEach(value -> addUnique(target.getExtractedFiles(), value));
    }
}
