package com.kafka.consumer.processors;

import com.kafka.consumer.events.UserEvent;
import com.kafka.consumer.hbase.UserEventHbaseWriter;
import com.google.gson.Gson;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public 	class ProcessorTask implements Runnable {

    private KafkaStream<byte[], byte[]> stream;
    private UserEventHbaseWriter userEventHbaseWriter;

    public ProcessorTask(KafkaStream<byte[], byte[]> stream, UserEventHbaseWriter writer) {
        super();
        this.stream = stream;
        this.userEventHbaseWriter = writer;
    }

    private volatile boolean stop = false;

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (!stop) {
            proccessMessage(it);
        }
    }


    private void proccessMessage(ConsumerIterator<byte[], byte[]> it) {
        try {
            MessageAndMetadata<byte[], byte[]> next = it.next();
            byte[] message = next.message();
            writeToHbase(message);
        }
        catch (Exception e) {
            log.warn("failed to process event", e);
        }
    }

    private void writeToHbase(byte[] message) {
        String messageStr = new String(message);
        log.info("new message: " + messageStr);
        UserEvent userEvent = new Gson().fromJson(messageStr, UserEvent.class);
        userEventHbaseWriter.writeToHbase(userEvent);
    }
}
