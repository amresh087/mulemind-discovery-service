package com.mulemind.discovery.client;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import com.mulemind.discovery.dto.AIRequest;
@FeignClient(name = "ai-service", url = "${ai.service.url}")
public interface AiTranformationServiceClient {
    
    @PostMapping("/ai/intent")
    public String getIntent(@RequestBody AIRequest request, @RequestParam(required = false) String sessionMode) ;
        
       
       
    

}

