package com.kafka.consumer.kafka;

import com.kafka.consumer.hbase.UserEventHbaseWriter;
import com.kafka.consumer.processors.ProcessorTask;
import com.github.racc.tscg.TypesafeConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Slf4j
public class UserEventKafkaReader {

    private ConsumerConnector consumerConnector;
    private String sourceTopic;
    private ExecutorService executorPool;

    private List<ProcessorTask> consumers = new ArrayList<>();

    private static final int NUM_OF_THREADS = 10;

    @Inject
    public UserEventKafkaReader(ConsumerConfig consumerConfig,
                                @TypesafeConfig("kafka.inputTopic") String sourceTopic) {
        this.consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        executorPool = Executors.newFixedThreadPool(
                NUM_OF_THREADS,
                new ThreadFactoryBuilder().setDaemon(false)
                        .setNameFormat("KafkaReader%d").build());
        this.sourceTopic = sourceTopic;
    }

    public void read(UserEventHbaseWriter userEventHbaseWriter) {
        try {
            List<KafkaStream<byte[], byte[]>> readStreams = getTopicStreams(consumerConnector);
            for (final KafkaStream<byte[], byte[]> stream : readStreams) {
                ProcessorTask procTask = new ProcessorTask(stream, userEventHbaseWriter);
                executorPool.submit(procTask);
            }

        } catch (Exception e) {
            log.warn("Unexpected error: " + e);
        }

    }

    private List<KafkaStream<byte[], byte[]>> getTopicStreams(ConsumerConnector consumerConnector) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(sourceTopic, new Integer(NUM_OF_THREADS));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(sourceTopic);
        return streams;
    }

}
