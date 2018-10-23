package com.kafka.consumer.events;


import com.kafka.consumer.enums.EventType;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class UserEvent {
    private String userId;
    private EventType eventType;
    private long timestamp; //timestamp in milliseconds
    private String ip;
    private Map<String, Object> params;

}
