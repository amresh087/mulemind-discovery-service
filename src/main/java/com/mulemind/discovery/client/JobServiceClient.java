package com.mulemind.discovery.client;
import java.util.Map;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import com.mulemind.discovery.dto.JobResponse;

@FeignClient(name = "mulemind-discovery-service", url = "${document.service.url}")
public interface JobServiceClient {

    @PutMapping("/documents/jobs/{id}/status")
    public JobResponse updateJobStatus(@PathVariable UUID id, @RequestBody Map<String, String> payload);
    
}

