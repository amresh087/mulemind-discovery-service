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
    public static Map<String, String> extractAllFiles(byte[] zipBytes) {

        Map<String, String> files = new LinkedHashMap<>();

        if (zipBytes == null || zipBytes.length == 0) {
            return files;
        }

        Path temporaryArchive = null;
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

    public static boolean isMacOsMetadataEntry(String fileName) {
        String normalizedName = fileName.replace('\\', '/');
        String fileNameOnly = normalizedName.substring(normalizedName.lastIndexOf('/') + 1);
        return normalizedName.equals("__MACOSX")
                || normalizedName.startsWith("__MACOSX/")
                || fileNameOnly.equals(".DS_Store")
                || fileNameOnly.startsWith("._");
    }

    private static byte[] sanitizeMalformedStoredEntries(byte[] zipBytes) {
        if (zipBytes.length < 30) {
            return zipBytes;
        }

        byte[] sanitized = zipBytes.clone();
        int offset = 0;
        while (offset <= sanitized.length - 30) {
            if (sanitized[offset] == 'P' && sanitized[offset + 1] == 'K'
                    && sanitized[offset + 2] == 0x03 && sanitized[offset + 3] == 0x04) {
                int flags = readUnsignedShort(sanitized, offset + 6);
                int compressionMethod = readUnsignedShort(sanitized, offset + 8);

                if (compressionMethod == 0 && (flags & 0x0008) != 0) {
                    int centralDirectoryOffset = findCentralDirectoryEntry(sanitized, offset);
                    if (centralDirectoryOffset >= 0) {
                        writeUnsignedShort(sanitized, offset + 6, flags & ~0x0008);
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
}