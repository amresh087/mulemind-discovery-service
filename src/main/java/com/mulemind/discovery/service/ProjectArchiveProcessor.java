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

import com.mulemind.discovery.util.ZipExtractorUtil;

@Component
public class ProjectArchiveProcessor {

    private static final Set<String> XML_EXTENSIONS = Set.of(".xml", ".xsd", ".wsdl");
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
            System.out.println("-------------- File: " + fileName + "--------------Content Length: " + content.length());
            analysis.getExtractedFiles().add(fileName);
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
    }

    /**
     * Analyzes a Mule flow XML file and extracts relevant information.
     * 
     * @param content
     * @param analysis
     */
    private void analyzeMuleFlowXml(String content, ProjectArtifactAnalysis analysis) {
        extractMuleFlows(content, analysis);
        extractHttpListeners(content, analysis);
        extractFlowReferences(content, analysis);
        extractTransformations(content, analysis);
        extractChoices(content, analysis);
        extractConnectors(content, analysis);
        extractKafkaTopics(content, analysis);
        extractMqEndpoints(content, analysis);
        extractDbOperations(content, analysis);
        extractFileOperations(content, analysis);
        extractErrorHandlers(content, analysis);
        extractVariables(content, analysis);
        extractSubflows(content, analysis);
    }

    private void extractMuleVersion(String content, ProjectArtifactAnalysis analysis) {
        addTagValues(content, "muleVersion", analysis.getMuleVersions());
        addTagValues(content, "mule.version", analysis.getMuleVersions());
        addDependencyVersion(content, "mule-runtime", analysis.getMuleVersions());
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
