package com.example.streamSet;

import com.example.streamSet.model.PipelineDetails;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class PiplineController {

    private PipelineService pipelineService;

    public PiplineController(
            PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @RequestMapping(path = "/pipeline/active", method = RequestMethod.GET)
    public boolean turnOnPipline(@RequestParam(value = "pipelineId") String pipelineId){

        return pipelineService.turnOnPipline(pipelineId);
    }

    @RequestMapping(path = "/pipeline/inactive", method = RequestMethod.GET)
    public boolean turnOffPipline(@RequestParam(value = "pipelineId") String pipelineId){

        return pipelineService.turnOffPipline(pipelineId);
    }

    @RequestMapping(path = "/pipeline/all", method = RequestMethod.GET)
    public List<PipelineDetails> getAllPipelines(){

        return pipelineService.getPipelines();
    }

}
