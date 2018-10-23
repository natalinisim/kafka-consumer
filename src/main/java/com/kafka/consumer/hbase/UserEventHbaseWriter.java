package com.kafka.consumer.hbase;

import com.kafka.consumer.events.UserEvent;

public interface UserEventHbaseWriter {

    void writeToHbase(UserEvent UserEvent);
}
