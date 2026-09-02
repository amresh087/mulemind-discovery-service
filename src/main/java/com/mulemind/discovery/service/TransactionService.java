// package com.mulemind.discovery.service;

// import java.io.ByteArrayInputStream;
// import java.io.InputStream;
// import java.nio.charset.StandardCharsets;
// import java.util.List;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;
// import org.springframework.util.StringUtils;

// import com.mulemind.discovery.client.DocumentServiceClient;
// import com.mulemind.discovery.dto.EdiDataEvent;
// import com.mulemind.discovery.kafka.DiscoveryKafkaProducer;
// import com.mulemind.discovery.dto.TransactionResponse;
// import com.mulemind.discovery.dto.TransactionTypeRequest;
// import com.mulemind.discovery.dto.TransactionTypeResponse;
// import com.mulemind.discovery.dto.TransformationEvent;
// import com.mulemind.discovery.dto.TransformationJobStatusRequest;
// import com.mulemind.discovery.edi.EdiConverter;
// import com.mulemind.discovery.entity.TransactionType;
// import com.mulemind.discovery.repository.TransactionTypeRepository;

// import io.minio.BucketExistsArgs;
// import io.minio.GetObjectArgs;
// import io.minio.MakeBucketArgs;
// import io.minio.MinioClient;
// import io.minio.PutObjectArgs;
// import lombok.RequiredArgsConstructor;

// @Service
// @RequiredArgsConstructor
// public class TransactionService {

//     private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

//         private final TransactionTypeRepository transactionTypeRepository;
//         private final MinioClient minioClient;
//         private final DiscoveryKafkaProducer discoveryKafkaProducer;
//         private final EdiConverter ediConverter;
//         private final DocumentServiceClient documentServiceClient;

//     @Value("${minio.bucket-name}")
//     private String bucketName;

//     @Value("${app.kafka.topic.edi-data-event:edi-data-event}")
//     private String ediDataEventTopic;

   
//     public List<TransactionResponse> getTransactions() {
//         return List.of(
//                 TransactionResponse.builder()
//                         .transactionId("TXN-1001")
//                         .customerId("CUST-001")
//                         .status("COMPLETED")
//                         .amount(129.99)
//                         .build(),
//                 TransactionResponse.builder()
//                         .transactionId("TXN-1002")
//                         .customerId("CUST-002")
//                         .status("PENDING")
//                         .amount(89.50)
//                         .build());
//     }

  

//     public List<TransactionTypeResponse> getTransactionTypes() {
//         return transactionTypeRepository.findAll().stream()
//                 .map(this::toResponse)
//                 .toList();
//     }

  


//     public TransactionTypeResponse createTransactionType(TransactionTypeRequest request) {
//         TransactionType entity = TransactionType.builder()
//                 .transactionCode(request.getTransactionCode())
//                 .documentName(request.getDocumentName())
//                 .purpose(request.getPurpose())
//                 .build();
//         return toResponse(transactionTypeRepository.save(entity));
//     }

   

//     public TransactionTypeResponse updateTransactionType(String transactionCode, TransactionTypeRequest request) {
//         TransactionType existing = transactionTypeRepository.findById(transactionCode)
//                 .orElseThrow(() -> new IllegalArgumentException("Transaction type not found: " + transactionCode));

//         existing.setDocumentName(request.getDocumentName());
//         existing.setPurpose(request.getPurpose());
//         return toResponse(transactionTypeRepository.save(existing));
//     }

//     public void deleteTransactionType(String transactionCode) {
//         transactionTypeRepository.deleteById(transactionCode);
//     }

   
//     public void processTransformationEvent(TransformationEvent event) {

//         if (event == null || !StringUtils.hasText(event.getObjectName())) {
//             log.warn("Transformation event has no object name, skipping MinIO download");
//             return;
//         }

//         try (InputStream objectStream = minioClient.getObject(
//                 GetObjectArgs.builder().bucket(bucketName).object(event.getObjectName())
//                         .build())) {

//             String fileContent = new String(objectStream.readAllBytes(), StandardCharsets.UTF_8);
//             log.info("Downloaded transformation file {} from MinIO bucket {}. Content length: {}",
//                     event.getObjectName(), bucketName, fileContent.length());

//             String xmlPayload = ediConverter.convertToXml(fileContent);
//             log.info("Converted EDI payload for {} into XML with {} characters", event.getObjectName(), xmlPayload.length());

//             EdiDataEvent ediDataEvent = EdiDataEvent.builder()
//                     .documentId(event.getDocumentId())
//                     .documentName(event.getDocumentName())
//                     .documentType(event.getDocumentType())
//                     .tenant(event.getTenant())
//                     .transactionTypeCode(event.getTransactionTypeCode())
//                     .mappingType(event.getMappingType())
//                     .status(event.getStatus())
//                     .objectName(event.getObjectName())
//                     .eventType(event.getEventType())
//                     .jobId(event.getJobId())
//                     .timestamp(event.getTimestamp() != null ? java.time.LocalDateTime.parse(event.getTimestamp()) : null)
//                     .payload(xmlPayload)
//                     .build();

//             saveEdiXmlToMinio(xmlPayload, ediDataEvent);

//             discoveryKafkaProducer.send(ediDataEvent, event.getDocumentId());

//             log.info("Published EDI data event to Kafka topic {} for object {}", ediDataEventTopic,
//                     event.getObjectName());
                    
//                         // Update transformation job status to indicate EDI XML was published
//                         if (event.getJobId() != null && !event.getJobId().isBlank()) {
//                                 try {
//                                         java.util.UUID jobUuid = java.util.UUID.fromString(event.getJobId());
//                                         TransformationJobStatusRequest req = TransformationJobStatusRequest.builder()
//                                                         .jobName(event.getDocumentName())
//                                                         .payload("EDI_XML_TO_IDOC_XML")
//                                                         .build();
//                                         documentServiceClient.updateJobStatus(jobUuid, req);
//                                 } catch (IllegalArgumentException ex) {
//                                         log.warn("Invalid jobId format when updating to EDI_XML_TO_IDOC_XML: {}", event.getJobId(), ex);
//                                 } catch (Exception ex) {
//                                         log.warn("Failed to update job status to EDI_XML_TO_IDOC_XML for job {}", event.getJobId(), ex);
//                                 }
//                         }
                    
//         } catch (Exception ex) {
//             log.error("Failed to read transformation file {} from MinIO", event.getObjectName(), ex);
//         }
//     }



//     private TransactionTypeResponse toResponse(TransactionType entity) {
//         return TransactionTypeResponse.builder()
//                 .transactionCode(entity.getTransactionCode())
//                 .documentName(entity.getDocumentName())
//                 .purpose(entity.getPurpose())
//                 .build();
//     }

//     private void saveEdiXmlToMinio(String xmlPayload, EdiDataEvent ediDataEvent) {
//         if (!StringUtils.hasText(xmlPayload) || ediDataEvent == null) {
//             log.warn("No EDI XML payload available to save to MinIO");
//             return;
//         }

//         try {
//             String resolvedBucketName = StringUtils.hasText(bucketName) ? bucketName : "documents";
//             String objectName = buildEdiXmlObjectName(ediDataEvent);

//             boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(resolvedBucketName).build());
//             if (!exists) {
//                 minioClient.makeBucket(MakeBucketArgs.builder().bucket(resolvedBucketName).build());
//             }

//             byte[] contentBytes = xmlPayload.getBytes(StandardCharsets.UTF_8);
//             minioClient.putObject(PutObjectArgs.builder()
//                     .bucket(resolvedBucketName)
//                     .object(objectName)
//                     .stream(new ByteArrayInputStream(contentBytes), contentBytes.length, -1)
//                     .contentType("application/xml")
//                     .build());

//             log.info("Saved EDI XML to MinIO bucket {}/{}", resolvedBucketName, objectName);
//         } catch (Exception ex) {
//             log.error("Failed to save EDI XML to MinIO for document {}", ediDataEvent.getDocumentId(), ex);
//         }
//     }

//     public String getXmlByDocumentAndType(String documentId, String xmlType) {
//         if (!StringUtils.hasText(documentId)) {
//             throw new IllegalArgumentException("documentId is required");
//         }
//         if (!StringUtils.hasText(xmlType)) {
//             throw new IllegalArgumentException("xmlType is required");
//         }

//         String objectName = resolveXmlObjectName(documentId, xmlType);

//         try (InputStream objectStream = minioClient.getObject(
//                 GetObjectArgs.builder().bucket(bucketName).object(objectName).build())) {
//             return new String(objectStream.readAllBytes(), StandardCharsets.UTF_8);
//         } catch (Exception ex) {
//             log.warn("Could not load XML from MinIO for {} at {}: {}", documentId, objectName, ex.getMessage());
//             return null;
//         }
//     }

//     static String resolveXmlObjectName(String documentId, String xmlType) {
//         String normalizedType = xmlType.trim().toLowerCase();
//         if ("edixml".equals(normalizedType)) {
//             return "edixml/" + documentId + ".xml";
//         }
//         if ("idocxml".equals(normalizedType)) {
//             return "inbound/" + documentId + ".xml";
//         }
//         throw new IllegalArgumentException("xmlType must be either 'edixml' or 'idocxml'");
//     }

//     private String buildEdiXmlObjectName(EdiDataEvent ediDataEvent) {
//         String documentId = StringUtils.hasText(ediDataEvent.getDocumentId()) ? ediDataEvent.getDocumentId() : "unknown-document";
//         return "edixml/" + documentId + ".xml";
//     }
// }
