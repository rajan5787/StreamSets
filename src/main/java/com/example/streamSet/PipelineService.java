package com.example.streamSet;

import com.example.streamSet.model.PipelineDetailResponse;
import com.example.streamSet.model.PipelineDetails;
import com.example.streamSet.model.PipelineStatusResponse;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;

@Service
public class PipelineService {

    @Value("${streamsets.base.url}")
    private String streamSetBaseUrl;

    private RestTemplate restTemplate;

    public PipelineService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private HttpHeaders createHttpHeaders(String user, String password)
    {
        String notEncoded = user + ":" + password;
        String encodedAuth = Base64.getEncoder().encodeToString(notEncoded.getBytes());
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Basic " + encodedAuth);
        headers.add("X-Requested-By","Data Collector");
        return headers;
    }

    public boolean turnOnPipline(String pipelineId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth("admin", "admin");
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Requested-By", "Data Collector");
        HttpEntity<String> request = new HttpEntity<String>(headers);
        try {
            ResponseEntity<PipelineStatusResponse> response = restTemplate.exchange(
                    streamSetBaseUrl+pipelineId+"/status?rev=0"
                    , HttpMethod.GET, request, PipelineStatusResponse.class);

            if(!response.getBody().status.equals("RUNNING")) {

                request = new HttpEntity<String>(null,headers);
                PipelineStatusResponse result = restTemplate.postForObject(
                        streamSetBaseUrl+pipelineId+"/start?rev=0"
                        , request, PipelineStatusResponse.class);

                return result.status.equals("STARTING") ? true : false;
            }
            else
                return true;
        }
        catch (Exception ex){
            return false;
        }

    }

    public boolean turnOffPipline(String pipelineId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth("admin", "admin");
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Requested-By", "Data Collector");
        HttpEntity<String> request = new HttpEntity<String>(headers);
        try {
            ResponseEntity<PipelineStatusResponse> response = restTemplate.exchange(
                    streamSetBaseUrl+pipelineId+"/status?rev=0"
                    , HttpMethod.GET, request, PipelineStatusResponse.class);

            if(!response.getBody().status.equals("STOPPED")) {

                request = new HttpEntity<String>(null,headers);
                PipelineStatusResponse result = restTemplate.postForObject(
                        streamSetBaseUrl+pipelineId+"/stop?rev=0"
                        , request, PipelineStatusResponse.class);

                return result.status.equals("STOPPED") ? true : false;
            }
            else
                return true;
        }
        catch (Exception ex){
            return false;
        }

    }



    public List<PipelineDetails> getPipelines(){
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth("admin", "admin");
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Requested-By", "Data Collector");
        HttpEntity<String> request = new HttpEntity<String>(headers);
        try {
            ResponseEntity<PipelineDetailResponse> response = restTemplate.exchange(
                    "http://localhost:18630/rest/v1/pipelines?len=-1&orderBy=NAME&order=ASC"
                    , HttpMethod.GET, request, PipelineDetailResponse.class);

            return response.getBody().pipelineDetailsList;
        }
        catch (Exception ex){
            return null;
        }

    }


    public  PipelineStatusResponse getStatus(String pipelineId){
        HttpHeaders headers = createHttpHeaders("admin","admin");
        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

        try {
            ResponseEntity<PipelineStatusResponse> response = restTemplate.exchange(
                    streamSetBaseUrl+pipelineId+"/status?rev=0"
                    , HttpMethod.GET, entity, PipelineStatusResponse.class);
            return response.getBody();

        }
        catch (Exception ex){

        }
        return null;
    }
}
