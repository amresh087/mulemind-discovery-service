package com.mulemind.discovery.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.stereotype.Component;

import com.mulemind.discovery.dto.ApiEndpoint;
import com.mulemind.discovery.dto.ApplicationDetails;
import com.mulemind.discovery.dto.ConnectorDetails;
import com.mulemind.discovery.dto.FlowDetail;
import com.mulemind.discovery.dto.FlowReference;
import com.mulemind.discovery.dto.FlowTrigger;
import com.mulemind.discovery.dto.RuntimeInfo;
import com.mulemind.discovery.dto.SourceFileDetails;
import com.mulemind.discovery.dto.TypeMetadata;
import com.mulemind.discovery.dto.TransformationDetail;
import com.mulemind.discovery.dto.VariableDetail;
import com.mulemind.discovery.util.ZipExtractorUtil;

@Component
public class ProjectArchiveProcessor {

    private static final Set<String> DWL_EXTENSIONS = Set.of(".dwl", ".dwl.xml");
    private static final Set<String> RAML_EXTENSIONS = Set.of(".raml", ".yaml", ".yml");
    private static final Set<String> PROPERTY_EXTENSIONS = Set.of(".properties", ".env", ".cfg");

    private static final Pattern API_PATTERN = Pattern.compile("(?:<path>|<uri>|@Path\\(|/[-A-Za-z0-9_./{}]+)");
    private static final Pattern KAFKA_PATTERN = Pattern.compile(
            "(?:kafka|topic|topicName|topic-name).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9._-]+)[\"']?",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MQ_PATTERN = Pattern.compile(
            "(?:mq|rabbitmq|amqp|jms|activemq|queue|exchange|destination).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9._:/-]+)[\"']?",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DB_PATTERN = Pattern.compile(
            "(?:jdbc:[A-Za-z0-9._:/+-]+|\\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE TABLE|ALTER TABLE|DROP TABLE|MERGE)\\b.*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "(?:file|path|directory|folder|input|output|location).*?(?:=|:|\\()\\s*[\"']?([A-Za-z0-9_./\\:-]+)[\"']?",
            Pattern.CASE_INSENSITIVE);

    /**
     * Extracts the contents of a ZIP archive to a specified directory.
     *
     * @param archiveBytes         the byte array representing the ZIP file
     * @param destinationDirectory the directory where the archive should be
     *                             extracted
     * @return a list of paths to the extracted files
     */
    public List<Path> extractArchive(byte[] archiveBytes, Path destinationDirectory) {
        if (archiveBytes == null || archiveBytes.length == 0 || destinationDirectory == null) {
            return List.of();
        }

        try {
            Files.createDirectories(destinationDirectory);
            Path destination = destinationDirectory.toAbsolutePath().normalize();
            List<Path> extractedPaths = new java.util.ArrayList<>();

            try (ZipInputStream zipInputStream = new ZipInputStream(
                    new ByteArrayInputStream(sanitizeMalformedStoredEntries(archiveBytes)))) {
                ZipEntry entry;
                while ((entry = zipInputStream.getNextEntry()) != null) {
                    if (entry.isDirectory() || ZipExtractorUtil.isMacOsMetadataEntry(entry.getName())) {
                        continue;
                    }

                    Path outputPath = destination.resolve(entry.getName().replace('\\', '/')).normalize();
                    if (!outputPath.startsWith(destination)) {
                        throw new IllegalStateException(
                                "ZIP entry is outside extraction directory: " + entry.getName());
                    }

                    Files.createDirectories(outputPath.getParent());
                    Files.write(outputPath, zipInputStream.readAllBytes());
                    extractedPaths.add(outputPath);
                }
            }
            return extractedPaths;
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to extract project archive", ex);
        }
    }

    /**
     * Parses the extracted files from the archive and analyzes their content.
     *
     * @param archiveBytes         the byte array representing the ZIP file
     * @param destinationDirectory the directory where the archive was extracted
     * @return a ProjectArtifactAnalysis object containing the analysis results
     */
    public ProjectArtifactAnalysis parseArchive(byte[] archiveBytes, Path destinationDirectory) {
        ProjectArtifactAnalysis analysis = new ProjectArtifactAnalysis();
        if (archiveBytes == null || archiveBytes.length == 0) {
            return analysis;
        }
        Map<String, String> files = ZipExtractorUtil.extractAllFiles(archiveBytes, destinationDirectory);

        for (Map.Entry<String, String> entry : files.entrySet()) {
            String fileName = entry.getKey();
            String content = entry.getValue();

            if (shouldSkipEntry(fileName)) {
                continue;
            }
            analysis.getExtractedFiles().add(fileName);
            analysis.getSourceFiles().add(fileName);
                analysis.getSourceFileDetails().add(SourceFileDetails.builder()
                    .name(Path.of(fileName).getFileName().toString())
                    .parsed(true)
                    .build());
            analyzeContent(fileName, content, analysis);
        }
        return analysis;
    }

    /**
     * Sanitizes malformed stored entries in the ZIP archive.
     *
     * @param archiveBytes the byte array representing the ZIP file
     * @return the sanitized byte array
     */
    private static byte[] sanitizeMalformedStoredEntries(byte[] archiveBytes) {
        if (archiveBytes == null || archiveBytes.length < 30) {
            return archiveBytes;
        }

        byte[] sanitized = archiveBytes.clone();
        int offset = 0;

        while (offset <= sanitized.length - 30) {
            if (sanitized[offset] == 'P' && sanitized[offset + 1] == 'K' && sanitized[offset + 2] == 0x03
                    && sanitized[offset + 3] == 0x04) {
                int generalPurposeBitFlag = readUnsignedShort(sanitized, offset + 6);
                int compressionMethod = readUnsignedShort(sanitized, offset + 8);

                if (compressionMethod == 0 && (generalPurposeBitFlag & 0x0008) != 0) {
                    int centralDirectoryOffset = findCentralDirectoryEntry(sanitized, offset);
                    if (centralDirectoryOffset >= 0) {
                        writeUnsignedShort(sanitized, offset + 6, generalPurposeBitFlag & ~0x0008);
                        writeUnsignedShort(sanitized, offset + 8,
                                readUnsignedShort(sanitized, centralDirectoryOffset + 10));
                        writeInt(sanitized, offset + 14, readInt(sanitized, centralDirectoryOffset + 16));
                        writeInt(sanitized, offset + 18, readInt(sanitized, centralDirectoryOffset + 20));
                        writeInt(sanitized, offset + 22, readInt(sanitized, centralDirectoryOffset + 24));
                    }
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

    private static void writeUnsignedShort(byte[] data, int offset, int value) {
        data[offset] = (byte) (value & 0xFF);
        data[offset + 1] = (byte) ((value >>> 8) & 0xFF);
    }

    private static void writeInt(byte[] data, int offset, int value) {
        data[offset] = (byte) (value & 0xFF);
        data[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        data[offset + 2] = (byte) ((value >>> 16) & 0xFF);
        data[offset + 3] = (byte) ((value >>> 24) & 0xFF);
    }

    /**
     * Finds the offset of the corresponding central directory entry for a given
     * local file header offset.
     *
     * @param data              the byte array representing the ZIP file
     * @param localHeaderOffset the offset of the local file header
     * @return the offset of the central directory entry, or -1 if not found
     */
    private static int findCentralDirectoryEntry(byte[] data, int localHeaderOffset) {
        for (int offset = 0; offset <= data.length - 46; offset++) {
            if (data[offset] == 'P' && data[offset + 1] == 'K'
                    && data[offset + 2] == 0x01 && data[offset + 3] == 0x02
                    && readInt(data, offset + 42) == localHeaderOffset) {
                return offset;
            }
        }
        return -1;
    }

    /**
     * Determines whether a ZIP entry should be skipped based on its file name.
     *
     * @param fileName the name of the ZIP entry
     * @return true if the entry should be skipped, false otherwise
     */
    private static boolean shouldSkipEntry(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return true;
        }

        String normalized = fileName.replace('\\', '/');
        String lowerName = normalized.toLowerCase(Locale.ROOT);

        if (lowerName.startsWith(".") || lowerName.contains("/.")) {
            return true;
        }

        // Skip test resources and test code
        if (lowerName.contains("/src/test/")) {
            return true;
        }

        // Skip autogenerated resources
        if (lowerName.contains("/autogenerated")) {
            return true;
        }

        // Skip log4j, logback, and other logging-related files
        if (lowerName.contains("log4j2") || lowerName.contains("log4j") || lowerName.contains("logback")
                || lowerName.contains("logging")) {
            return true;
        }
        // Skip documentation files
        if (lowerName.contains("docs") || lowerName.contains("readme") || lowerName.contains("changelog")
                || lowerName.contains("license") || lowerName.contains("contributing") || lowerName.contains("docs")) {
            return true;
        }

        String fileSegment = lowerName.substring(lowerName.lastIndexOf('/') + 1);
        if (fileSegment.startsWith(".")) {
            return true;
        }

        return lowerName.endsWith(".class");
    }

    /**
     * 
     * @param fileName
     * @param content
     * @param analysis
     */
    private void analyzeContent(String fileName, String content, ProjectArtifactAnalysis analysis) {
        String normalizedName = fileName.toLowerCase(Locale.ROOT);

        // ---------------------------------------------------------
        // 1. pom.xml
        // ---------------------------------------------------------
        if (normalizedName.endsWith("/pom.xml") || normalizedName.equals("pom.xml")) {
            analyzePom(content, analysis);
            return;
        }

        // ---------------------------------------------------------
        // 2. mule-artifact.json
        // ---------------------------------------------------------
        if (normalizedName.endsWith("mule-artifact.json")) {
            analyzeMuleArtifact(content, analysis);
            return;
        }

        if (normalizedName.endsWith("application-types.xml")) {
            analyzeApplicationTypes(content, analysis);
            return;
        }

        // ---------------------------------------------------------
        // 3. Mule flow XML
        // src/main/mule/**/*.xml
        // ---------------------------------------------------------
        if (normalizedName.contains("/src/main/mule/") && normalizedName.endsWith(".xml")) {
            analyzeMuleFlowXml(content, analysis);
            return;
        }

        // ---------------------------------------------------------
        // 5. DataWeave
        // ---------------------------------------------------------
        if (isDwlFile(normalizedName)) {

            extractKafkaTopics(content, analysis);
            extractMqEndpoints(content, analysis);
            extractDbOperations(content, analysis);
            extractFileOperations(content, analysis);
            return;
        }

        // ---------------------------------------------------------
        // 6. RAML / OpenAPI
        // ---------------------------------------------------------
        if (isRamlOrYaml(normalizedName) || normalizedName.contains("swagger") || normalizedName.contains("openapi")) {
            extractApiEndpoints(content, analysis);
            return;
        }

        // ---------------------------------------------------------
        // 7. Properties / configuration
        // ---------------------------------------------------------
        if (isPropertyFile(normalizedName)) {
            extractKafkaTopics(content, analysis);
            extractMqEndpoints(content, analysis);
            extractDbOperations(content, analysis);
            extractFileOperations(content, analysis);
            return;
        }

    }

    /**
     * Analyzes a pom.xml file and extracts relevant information.
     * 
     * @param content
     * @param analysis
     */
    private void analyzePom(String content, ProjectArtifactAnalysis analysis) {
        analysis.setApplication(ApplicationDetails.builder()
            .name(firstTagValue(content, "name"))
            .groupId(firstTagValue(content, "groupId"))
            .artifactId(firstTagValue(content, "artifactId"))
            .version(firstTagValue(content, "version"))
            .packaging(firstTagValue(content, "packaging"))
            .muleRuntime(firstPropertyValue(content, "app.runtime"))
            .muleMavenPluginVersion(firstPropertyValue(content, "mule.maven.plugin.version"))
                .minMuleVersion("")
                .javaSpecificationVersions(new java.util.ArrayList<>())
            .build());
        extractPomConnectors(content, analysis);
        analysis.setDependencyDetails(com.mulemind.discovery.dto.DependencyDetails.builder()
            .http(analysis.getDependencyDetails().getHttp())
            .sockets(analysis.getDependencyDetails().getSockets())
            .build());
        extractMuleVersion(content, analysis);
        extractJavaVersion(content, analysis);
        extractDependencies(content, analysis);
        extractMulePlugins(content, analysis);
        extractConnectors(content, analysis);
    }

    /**
     * 
     * @param content
     * @param analysis
     */

    private void analyzeMuleArtifact(String content, ProjectArtifactAnalysis analysis) {
        extractApplicationName(content, analysis);
        extractMuleRuntime(content, analysis);
        extractArtifactProperties(content, analysis);
        String minMuleVersion = firstJsonValue(content, "minMuleVersion");
        List<String> javaVersions = jsonArrayValues(content, "javaSpecificationVersions");
        ApplicationDetails application = analysis.getApplication();
        if (application == null) {
            application = ApplicationDetails.builder().build();
        }
        application.setMinMuleVersion(minMuleVersion);
        application.setJavaSpecificationVersions(javaVersions);
        analysis.setApplication(application);
        analysis.setRuntimeInfo(RuntimeInfo.builder()
                .minMuleVersion(minMuleVersion)
                .javaSpecificationVersions(javaVersions)
                .build());
    }

    private void analyzeApplicationTypes(String content, ProjectArtifactAnalysis analysis) {
        TypeMetadata.PayloadMetadata inputPayload = payloadMetadata(content, "Input-Payload");
        TypeMetadata.PayloadMetadata outputPayload = payloadMetadata(content, "Output-Payload");
        TypeMetadata.AttributeMetadata inputAttributes = attributeMetadata(content, "Input-Attributes");
        TypeMetadata.AttributeMetadata outputAttributes = attributeMetadata(content, "Output-Attributes");
        analysis.setTypeMetadata(TypeMetadata.builder()
                .inputPayload(inputPayload)
                .outputPayload(outputPayload)
                .inputAttributes(inputAttributes)
                .outputAttributes(outputAttributes)
                .build());
    }

    /**
     * Analyzes a Mule flow XML file and extracts relevant information.
     * 
     * @param content
     * @param analysis
     */
    private void analyzeMuleFlowXml(String content, ProjectArtifactAnalysis analysis) {
        extractStructuredMuleDetails(content, analysis);
        extractMuleFlows(content, analysis);
        extractHttpListeners(content, analysis);
        addAttributeValues(content, "path", analysis.getApis(), "listener", "request");
        extractFlowReferences(content, analysis);
        extractTransformations(content, analysis);
        extractChoices(content, analysis);
        extractConnectors(content, analysis);
        extractKafkaTopics(content, analysis);
        extractMqEndpoints(content, analysis);
        extractDbOperations(content, analysis);
        extractFileOperations(content, analysis);
        analysis.getFileOperations().removeIf(fileOperation -> analysis.getHttpListeners().stream()
            .anyMatch(listener -> listener.equalsIgnoreCase(fileOperation)));
        extractErrorHandlers(content, analysis);
        extractVariables(content, analysis);
        extractSubflows(content, analysis);
    }

    private void extractStructuredMuleDetails(String content, ProjectArtifactAnalysis analysis) {
        Map<String, String> listenerConfigs = new java.util.HashMap<>();
        Matcher configMatcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?listener-config\\b([^>]*)>(.*?)</(?:[A-Za-z_][\\w.-]*:)?listener-config\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        while (configMatcher.find()) {
            String configName = attribute(configMatcher.group(1), "name");
            Matcher connectionMatcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?listener-connection\\b([^>]*)/?>",
                    Pattern.CASE_INSENSITIVE).matcher(configMatcher.group(2));
            if (connectionMatcher.find()) {
                listenerConfigs.put(configName, connectionMatcher.group(1));
            }
        }

        Matcher flowMatcher = Pattern.compile("<(?:(?:[A-Za-z_][\\w.-]*):)?(flow|sub-flow)\\b([^>]*)>(.*?)</(?:(?:[A-Za-z_][\\w.-]*):)?\\1\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        while (flowMatcher.find()) {
            String flowType = flowMatcher.group(1).toUpperCase(Locale.ROOT);
            String flowAttributes = flowMatcher.group(2);
            String flowBody = flowMatcher.group(3);
            String flowName = attribute(flowAttributes, "name");
                FlowDetail flow = FlowDetail.builder()
                    .name(flowName)
                        .type(flowType)
                        .build();

            Matcher processorMatcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?([A-Za-z_][\\w.-]*)(?:\\s[^>]*)?/?>",
                    Pattern.CASE_INSENSITIVE).matcher(flowBody);
            while (processorMatcher.find()) {
                String processor = processorMatcher.group(1);
                if (Set.of("listener", "message", "set-payload", "mule", "doc").contains(processor.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                flow.getProcessors().add(processor.equalsIgnoreCase("transform") ? "transform" : processor);
            }

            Matcher referenceMatcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?flow-ref\\b([^>]*)/?>",
                    Pattern.CASE_INSENSITIVE).matcher(flowBody);
            while (referenceMatcher.find()) {
                String target = attribute(referenceMatcher.group(1), "name");
                addUnique(flow.getReferences(), target);
                analysis.getFlowReferenceDetails().add(FlowReference.builder()
                        .sourceFlow(flowName).targetFlow(target).build());
            }

            Matcher listenerMatcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?listener\\b([^>]*)/?>",
                    Pattern.CASE_INSENSITIVE).matcher(flowBody);
            if (listenerMatcher.find()) {
                String listenerAttributes = listenerMatcher.group(1);
                String configName = attribute(listenerAttributes, "config-ref");
                String path = attribute(listenerAttributes, "path");
                String configuredMethod = nullIfBlank(attribute(listenerAttributes, "method"));
                String method = configuredMethod == null ? "GET" : configuredMethod;
                String connection = listenerConfigs.getOrDefault(configName, "");
                flow.setTrigger(FlowTrigger.builder().type("HTTP").path(path).build());
                analysis.getApiDetails().add(ApiEndpoint.builder()
                    .type(resolveListenerType(content, listenerAttributes))
                    .method(method)
                    .methodRestricted(configuredMethod != null)
                        .path(path)
                        .listenerConfig(configName)
                        .host(attribute(connection, "host"))
                        .port(parsePort(attribute(connection, "port")))
                        .flow(flowName)
                        .build());
            }

            analysis.getFlowDetails().add(flow);
            extractStructuredVariables(flowBody, flowName, analysis);
            extractStructuredTransformations(flowBody, flowName, analysis);
        }
    }

    private void extractStructuredVariables(String flowBody, String flowName, ProjectArtifactAnalysis analysis) {
        Matcher matcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?set-variable\\b([^>]*)/?>",
                Pattern.CASE_INSENSITIVE).matcher(flowBody);
        while (matcher.find()) {
            String attributes = matcher.group(1);
            analysis.getVariableDetails().add(VariableDetail.builder()
                    .name(attribute(attributes, "variableName"))
                    .expression(attribute(attributes, "value"))
                    .flow(flowName)
                    .build());
        }
    }

    private void extractStructuredTransformations(String flowBody, String flowName,
            ProjectArtifactAnalysis analysis) {
        Matcher matcher = Pattern.compile("<(?:(?:[A-Za-z_][\\w.-]*):)?transform\\b[^>]*>.*?<(?:(?:[A-Za-z_][\\w.-]*):)?set-payload\\b[^>]*>\\s*<!\\[CDATA\\[(.*?)\\]\\]>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(flowBody);
        while (matcher.find()) {
            String script = matcher.group(1).trim();
            String[] scriptLines = script.split("---", 2);
            String body = scriptLines.length == 2 ? scriptLines[1].trim() : script;
            Matcher outputMatcher = Pattern.compile("output\\s+([^\\s]+)", Pattern.CASE_INSENSITIVE).matcher(script);
            analysis.getTransformationDetails().add(TransformationDetail.builder()
                    .type("DataWeave")
                    .flow(flowName)
                    .outputMimeType(outputMatcher.find() ? outputMatcher.group(1) : "")
                    .logic(body.replaceAll("\\s+", " ").trim())
                    .build());
        }
    }

    private static String attribute(String attributes, String name) {
        Matcher matcher = Pattern.compile("(?<![\\w:.-])" + Pattern.quote(name) + "\\s*=\\s*[\\\"']([^\\\"']*)[\\\"']",
                Pattern.CASE_INSENSITIVE).matcher(attributes == null ? "" : attributes);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    private static String nullIfBlank(String value) {
        return value.isBlank() ? null : value;
    }

    private static Integer parsePort(String value) {
        try {
            return value.isBlank() ? null : Integer.valueOf(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private void extractMuleVersion(String content, ProjectArtifactAnalysis analysis) {
        addTagValues(content, "muleVersion", analysis.getMuleVersions());
        addTagValues(content, "mule.version", analysis.getMuleVersions());
        addDependencyVersion(content, "mule-runtime", analysis.getMuleVersions());
    }

    private void extractPomConnectors(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = Pattern.compile("<(?:[A-Za-z_][\\w.-]*:)?dependency\\b[^>]*>(.*?)</(?:[A-Za-z_][\\w.-]*:)?dependency\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        while (matcher.find()) {
            String block = matcher.group(1);
            String artifact = firstTagValue(block, "artifactId");
            String version = firstTagValue(block, "version");
            String type = connectorType(artifact);
            if (type != null) {
                analysis.getConnectorDetails().add(ConnectorDetails.builder().type(type).version(version).build());
                String dependency = artifact + ":" + version;
                addConnectorDependency(analysis, type, dependency);
            }
        }
    }

    private static String resolveListenerType(String content, String listenerAttributes) {
        String configRef = attribute(listenerAttributes, "config-ref");
        Pattern pattern = Pattern.compile("<([A-Za-z_][\\w.-]*):listener\\b[^>]*\\bconfig-ref\\s*=\\s*[\\\"']"
                + Pattern.quote(configRef) + "[\\\"']", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(content);
        return matcher.find() ? matcher.group(1).toUpperCase(Locale.ROOT) : "HTTP";
    }

    private static String connectorType(String artifact) {
        if (artifact == null || artifact.isBlank()) {
            return null;
        }
        String normalized = artifact.toLowerCase(Locale.ROOT);
        int connectorIndex = normalized.lastIndexOf("-connector");
        if (connectorIndex < 0) {
            return null;
        }
        String type = artifact.substring(0, connectorIndex);
        int separator = type.lastIndexOf('-');
        return (separator >= 0 ? type.substring(separator + 1) : type).toUpperCase(Locale.ROOT);
    }

    private static void addConnectorDependency(ProjectArtifactAnalysis analysis, String type, String dependency) {
        if ("HTTP".equals(type)) {
            addUnique(analysis.getDependencyDetails().getHttp(), dependency);
        } else if ("SOCKETS".equals(type)) {
            addUnique(analysis.getDependencyDetails().getSockets(), dependency);
        }
    }

    private static String firstPropertyValue(String content, String property) {
        Matcher matcher = Pattern.compile("<" + Pattern.quote(property) + ">\\s*([^<]+)\\s*</" + Pattern.quote(property) + ">",
                Pattern.CASE_INSENSITIVE).matcher(content);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    private static String firstJsonValue(String content, String key) {
        Matcher matcher = Pattern.compile("[\\\"']" + Pattern.quote(key)
                + "[\\\"']\\s*:\\s*[\\\"']([^\\\"']+)[\\\"']", Pattern.CASE_INSENSITIVE).matcher(content);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    private static List<String> jsonArrayValues(String content, String key) {
        Matcher arrayMatcher = Pattern.compile("[\\\"']" + Pattern.quote(key)
                + "[\\\"']\\s*:\\s*\\[(.*?)]", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        if (!arrayMatcher.find()) {
            return new java.util.ArrayList<>();
        }
        List<String> values = new java.util.ArrayList<>();
        Matcher valueMatcher = Pattern.compile("[\\\"']([^\\\"']+)[\\\"']").matcher(arrayMatcher.group(1));
        while (valueMatcher.find()) {
            addUnique(values, valueMatcher.group(1));
        }
        return values;
    }

    private static TypeMetadata.PayloadMetadata payloadMetadata(String content, String suffix) {
        Matcher matcher = Pattern.compile("<types:type\\b[^>]*\\bname\\s*=\\s*[\\\"'][^\\\"']*"
                + Pattern.quote(suffix) + "[\\\"'][^>]*>(.*?)</types:type\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        if (!matcher.find()) {
            return null;
        }
        String block = matcher.group(1);
        String name = suffix;
        String format = firstAttributeValue(matcher.group(), "format");
        String declaration = cdataContent(block);
        int nestedTypeStart = declaration.indexOf("\ntype org_");
        if (nestedTypeStart >= 0) {
            declaration = declaration.substring(0, nestedTypeStart);
        }
        String type = declaration.matches("(?s).*\\s=\\s*Any\\b.*") ? "Any" : null;
        java.util.Map<String, String> schema = new java.util.LinkedHashMap<>();
        Matcher fieldMatcher = Pattern.compile("\\b([A-Za-z][\\w]*)\\s*:\\s*([A-Za-z][\\w]*)").matcher(declaration);
        while (fieldMatcher.find()) {
            schema.put(fieldMatcher.group(1), fieldMatcher.group(2));
        }
        return TypeMetadata.PayloadMetadata.builder().name(name).format(format).type(type).schema(schema.isEmpty() ? null : schema).build();
    }

    private static TypeMetadata.AttributeMetadata attributeMetadata(String content, String suffix) {
        Matcher matcher = Pattern.compile("<types:type\\b[^>]*\\bname\\s*=\\s*[\\\"'][^\\\"']*"
                + Pattern.quote(suffix) + "[\\\"'][^>]*>(.*?)</types:type\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
        if (!matcher.find()) {
            return null;
        }
        String block = matcher.group(1);
        String declaration = cdataContent(block);
        int nestedTypeStart = declaration.indexOf("\ntype org_");
        if (nestedTypeStart >= 0) {
            declaration = declaration.substring(0, nestedTypeStart);
        }
        Matcher typeMatcher = Pattern.compile("typeAlias\\\"\\s*:\\s*\\\"([^\\\"]+)").matcher(block);
        String type = "";
        while (typeMatcher.find()) {
            type = typeMatcher.group(1);
            if (type.toLowerCase(Locale.ROOT).contains("httprequestattributes")) {
                break;
            }
        }
        if (type.isBlank()) {
            type = "HttpRequestAttributes";
        }
        List<String> fields = new java.util.ArrayList<>();
        Matcher fieldMatcher = Pattern.compile("\\b([A-Za-z][\\w]*)\\??\\s*:").matcher(declaration);
        while (fieldMatcher.find()) {
            addUnique(fields, fieldMatcher.group(1));
        }
        return TypeMetadata.AttributeMetadata.builder().type(type).fields(fields).build();
    }

    private static String cdataContent(String content) {
        Matcher matcher = Pattern.compile("<!\\[CDATA\\[(.*?)\\]\\]>", Pattern.DOTALL).matcher(content);
        return matcher.find() ? matcher.group(1) : content;
    }

    private static String firstAttributeValue(String content, String attribute) {
        Matcher matcher = Pattern.compile("\\b" + Pattern.quote(attribute) + "\\s*=\\s*[\\\"']([^\\\"']+)[\\\"']",
                Pattern.CASE_INSENSITIVE).matcher(content);
        return matcher.find() ? matcher.group(1) : "";
    }

    private void extractJavaVersion(String content, ProjectArtifactAnalysis analysis) {
        for (String tag : List.of("java.version", "maven.compiler.release", "maven.compiler.source",
                "maven.compiler.target")) {
            addTagValues(content, tag, analysis.getJavaVersions());
        }
    }

    private void extractDependencies(String content, ProjectArtifactAnalysis analysis) {
        addDependencyCoordinates(content, "dependency", analysis.getDependencies());
    }

    private void extractMulePlugins(String content, ProjectArtifactAnalysis analysis) {
        addDependencyCoordinates(content, "plugin", analysis.getMulePlugins());
    }

    private void extractConnectors(String content, ProjectArtifactAnalysis analysis) {
        List<String> artifacts = new java.util.ArrayList<>();
        addTagValues(content, "artifactId", artifacts);
        for (String artifact : artifacts) {
            String lower = artifact.toLowerCase(Locale.ROOT);
            if (lower.contains("connector") || lower.contains("transport") || lower.contains("module")
                    || lower.matches(".*(kafka|amqp|rabbitmq|jms|db|http|file|sftp|ftp|vm).*")) {
                addUnique(analysis.getConnectors(), artifact);
            }
        }
    }

    private void extractApplicationName(String content, ProjectArtifactAnalysis analysis) {
        addJsonValues(content, "name", analysis.getApplicationNames());
        addJsonValues(content, "artifactId", analysis.getApplicationNames());
        addTagValues(content, "name", analysis.getApplicationNames());
        addTagValues(content, "artifactId", analysis.getApplicationNames());
    }

    private void extractMuleRuntime(String content, ProjectArtifactAnalysis analysis) {
        addJsonValues(content, "minMuleVersion", analysis.getMuleRuntimes());
        addJsonValues(content, "runtimeVersion", analysis.getMuleRuntimes());
        addJsonValues(content, "runtime", analysis.getMuleRuntimes());
        addTagValues(content, "runtimeVersion", analysis.getMuleRuntimes());
        addTagValues(content, "runtime", analysis.getMuleRuntimes());
    }

    private void extractArtifactProperties(String content, ProjectArtifactAnalysis analysis) {
        addJsonValues(content, "minMuleVersion", analysis.getArtifactProperties());
        addJsonValues(content, "secureProperties", analysis.getArtifactProperties());
        addJsonValues(content, "configs", analysis.getArtifactProperties());
        addTagValues(content, "minMuleVersion", analysis.getArtifactProperties());
        Pattern matcherPattern = Pattern.compile("\\b([A-Za-z][\\w.-]*)\\s*=\\s*[\"']([^\"']+)[\"']");
        Matcher matcher = matcherPattern.matcher(content);
        while (matcher.find()) {
            if (matcher.group(1).toLowerCase(Locale.ROOT).contains("property")) {
                addUnique(analysis.getArtifactProperties(), matcher.group(1) + "=" + matcher.group(2));
            }
        }
    }

    private void extractMuleFlows(String content, ProjectArtifactAnalysis analysis) {
        addAttributeValues(content, "name", analysis.getMuleFlows(), "flow", "sub-flow");
    }

    private void extractHttpListeners(String content, ProjectArtifactAnalysis analysis) {
        addAttributeValues(content, "path", analysis.getHttpListeners(), "listener", "request");
        addAttributeValues(content, "config-ref", analysis.getHttpListeners(), "listener", "request");
    }

    private void extractFlowReferences(String content, ProjectArtifactAnalysis analysis) {
        addAttributeValues(content, "name", analysis.getFlowReferences(), "flow-ref");
    }

    private void extractTransformations(String content, ProjectArtifactAnalysis analysis) {
        addTagNames(content, analysis.getTransformations(), "transform", "ee:transform", "dataweave");
    }

    private void extractChoices(String content, ProjectArtifactAnalysis analysis) {
        addTagNames(content, analysis.getChoices(), "choice", "when", "otherwise");
    }

    private void extractErrorHandlers(String content, ProjectArtifactAnalysis analysis) {
        addTagNames(content, analysis.getErrorHandlers(), "error-handler", "on-error-continue", "on-error-propagate");
    }

    private void extractVariables(String content, ProjectArtifactAnalysis analysis) {
        addAttributeValues(content, "variableName", analysis.getVariables(), "set-variable");
        addAttributeValues(content, "name", analysis.getVariables(), "set-variable");
    }

    private void extractSubflows(String content, ProjectArtifactAnalysis analysis) {
        addAttributeValues(content, "name", analysis.getSubflows(), "sub-flow");
    }

    private static void addTagValues(String content, String tag, List<String> values) {
        String tagPattern = "(?:[A-Za-z_][\\w.-]*:)?" + Pattern.quote(tag);
        Pattern pattern = Pattern.compile("<" + tagPattern + "\\b[^>]*>(.*?)</" + tagPattern + "\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).replaceAll("<[^>]+>", "").trim();
            addUnique(values, value);
        }
    }

    private static void addJsonValues(String content, String key, List<String> values) {
        Pattern pattern = Pattern.compile("[\"']" + Pattern.quote(key)
                + "[\"']\\s*:\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            addUnique(values, matcher.group(1));
        }
    }

    private static void addDependencyCoordinates(String content, String element, List<String> values) {
        String elementPattern = "(?:[A-Za-z_][\\w.-]*:)?" + element;
        Pattern pattern = Pattern.compile("<" + elementPattern + "\\b[^>]*>(.*?)</" + elementPattern + "\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String block = matcher.group(1);
            String groupId = firstTagValue(block, "groupId");
            String artifactId = firstTagValue(block, "artifactId");
            String version = firstTagValue(block, "version");
            if (!groupId.isBlank() && !artifactId.isBlank()) {
                addUnique(values, groupId + ":" + artifactId + (version.isBlank() ? "" : ":" + version));
            }
        }
    }

    private static String firstTagValue(String content, String tag) {
        List<String> values = new java.util.ArrayList<>();
        addTagValues(content, tag, values);
        return values.isEmpty() ? "" : values.get(0);
    }

    private static void addDependencyVersion(String content, String artifactId, List<String> values) {
        Pattern pattern = Pattern.compile("<(?:(?:[A-Za-z_][\\w.-]*):)?artifactId\\b[^>]*>\\s*"
                + Pattern.quote(artifactId)
                + "\\s*</(?:(?:[A-Za-z_][\\w.-]*):)?artifactId>(.*?)</(?:(?:[A-Za-z_][\\w.-]*):)?dependency>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            addTagValues(matcher.group(1), "version", values);
        }
    }

    private static void addAttributeValues(String content, String attribute, List<String> values, String... elements) {
        for (String element : elements) {
            String elementPattern = "(?:[A-Za-z_][\\w.-]*:)?" + Pattern.quote(element);
            Pattern pattern = Pattern.compile("<" + elementPattern + "\\b[^>]*\\b" + Pattern.quote(attribute)
                    + "\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(content);
            while (matcher.find()) {
                addUnique(values, matcher.group(1));
            }
        }
    }

    private static void addTagNames(String content, List<String> values, String... names) {
        for (String name : names) {
            if (Pattern.compile("</?" + Pattern.quote(name) + "(?:\\s|>)", Pattern.CASE_INSENSITIVE)
                    .matcher(content).find()) {
                addUnique(values, name);
            }
        }
    }

    /**
     * 
     * @param content
     * @param analysis
     */
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

    /**
     * 
     * @param content
     * @param analysis
     */
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
            if (content.toLowerCase(Locale.ROOT).contains("topic")
                    && (value.contains(".") || value.contains("-") || value.contains("_"))) {
                addUnique(analysis.getKafkaTopics(), value);
            }
        }
    }

    /**
     * 
     * @param content
     * @param analysis
     */
    private void extractMqEndpoints(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = MQ_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                addUnique(analysis.getMqEndpoints(), value);
            }
        }
    }

    /**
     * 
     * @param content
     * @param analysis
     */
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

    /**
     * 
     * @param content
     * @param analysis
     */
    private void extractFileOperations(String content, ProjectArtifactAnalysis analysis) {
        Matcher matcher = FILE_PATTERN.matcher(content);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                addUnique(analysis.getFileOperations(), value);
            }
        }

        if (content.contains("file://") || content.contains("/tmp") || content.contains("/opt")
                || content.contains("/var/")) {
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

    /**
     * 
     * @param normalizedName
     * @return
     */
    private static boolean isDwlFile(String normalizedName) {
        return DWL_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    /**
     * 
     * @param normalizedName
     * @return
     */
    private static boolean isRamlOrYaml(String normalizedName) {
        return RAML_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    /**
     * 
     * @param normalizedName
     * @return
     */
    private static boolean isPropertyFile(String normalizedName) {
        return PROPERTY_EXTENSIONS.stream().anyMatch(normalizedName::endsWith);
    }

    /**
     * Adds a value to the list if it's not already present (case-insensitive).
     *
     * @param values the list of values
     * @param value  the value to add
     */
    private static void addUnique(List<String> values, String value) {
        if (value == null || value.isBlank()) {
            return;
        }

        String normalized = value.trim();
        if (values.stream().noneMatch(existing -> existing.equalsIgnoreCase(normalized))) {
            values.add(normalized);
        }
    }

}
