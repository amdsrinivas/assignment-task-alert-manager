package com.assignment.taskmanager.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

public class TaskStatus {
    private String id ;
    private long startTime ;
    private long endTime ;
    private String host ;
    private String type ;

    public TaskStatus() {
    }

    public TaskStatus(String id, long startTime, long endTime, String host, String type) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.host = host;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void fromString(String jsonString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper() ;
        Map<String, Object> jsonObject = new HashMap<>() ;
        String state ;
        jsonObject = mapper.readValue(jsonString, new TypeReference<Map<String, Object>>(){} ) ;

        this.id = (String) jsonObject.get("id");
        state = (String) jsonObject.get("state");

        if(state.equals("STARTED")){
            this.startTime = (long) jsonObject.get("timestamp");
        }
        else{
            this.endTime = (long) jsonObject.get("timestamp") ;
        }

        if(jsonObject.containsKey("type")){
            this.type = (String) jsonObject.get("type") ;
        }

        if(jsonObject.containsKey("host")){
            this.host = (String) jsonObject.get("host") ;
        }

    }

}
