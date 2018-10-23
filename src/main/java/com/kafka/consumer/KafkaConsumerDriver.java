package com.kafka.consumer;

import com.kafka.consumer.hbase.UserEventHbaseWriter;
import com.kafka.consumer.kafka.UserEventKafkaReader;
import com.github.racc.tscg.TypesafeConfigModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaConsumerDriver {

    private final UserEventKafkaReader userEventKafkaReader;
    private final UserEventHbaseWriter userEventHbaseWriter;

    @Inject
    public KafkaConsumerDriver(UserEventKafkaReader kafkaReader, UserEventHbaseWriter userEventHbaseWriter) {
        this.userEventKafkaReader = kafkaReader;
        this.userEventHbaseWriter = userEventHbaseWriter;

    }

    public static void main(String[] args) throws Exception {
        log.info("Starting KafkaConsumerDriver");

        Config config = ConfigFactory.load();

        Injector injector = Guice.createInjector(
                TypesafeConfigModule.fromConfigWithPackage(config, "com.kafka.consumer"),
                new KafkaConsumerModule(config)
        );

        KafkaConsumerDriver main = injector.getInstance(KafkaConsumerDriver.class);

        try {
            main.start();
        } catch (Exception e) {
            log.error("main cannot start, got exception", e);
        }
    }

    public void start() {
        userEventKafkaReader.read(userEventHbaseWriter);
    }



}

