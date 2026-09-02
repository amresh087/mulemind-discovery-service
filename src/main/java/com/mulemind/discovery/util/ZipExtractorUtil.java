package com.mulemind.discovery.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public final class ZipExtractorUtil {

    private ZipExtractorUtil() {
    }

    /**
     * Returns all files from zip including files inside subfolders.
     *
     * Key = file path in zip
     * Value = file content as String
     */
    public static Map<String, String> extractAllFiles(byte[] zipBytes, Path temporaryArchive) {

        Map<String, String> files = new LinkedHashMap<>();

        if (zipBytes == null || zipBytes.length == 0) {
            return files;
        }

       // Path temporaryArchive = null;
        try {
            temporaryArchive = Files.createTempFile("mulemind-archive-", ".zip");
            Files.write(temporaryArchive, zipBytes);
            try (ZipFile zipFile = new ZipFile(temporaryArchive.toFile())) {
                var entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                if (entry.isDirectory() || isMacOsMetadataEntry(entry.getName())) {
                    continue;
                }
                String fileName = entry.getName();
                System.out.println("Processing: " + fileName);
                try (var inputStream = zipFile.getInputStream(entry)) {
                    files.put(fileName, new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
                }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract zip", e);
        } finally {
            if (temporaryArchive != null) {
                try {
                    Files.deleteIfExists(temporaryArchive);
                } catch (IOException ignored) {
                }
            }
        }

        return files;
    }

    /**
     * Checks if the given file name corresponds to a macOS metadata entry.
     * @param fileName
     * @return
     */
    public static boolean isMacOsMetadataEntry(String fileName) {
        String normalizedName = fileName.replace('\\', '/');
        String fileNameOnly = normalizedName.substring(normalizedName.lastIndexOf('/') + 1);
        return normalizedName.equals("__MACOSX")
                || normalizedName.startsWith("__MACOSX/")
                || fileNameOnly.equals(".DS_Store")
                || fileNameOnly.startsWith("._");
    }

   

}