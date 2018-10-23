package com.kafka.consumer;

import com.kafka.consumer.hbase.UserEventHbaseWriter;
import com.kafka.consumer.hbase.UserEventHbaseWriterImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import kafka.consumer.ConsumerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.util.Properties;



@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerModule extends AbstractModule {
    private final Config config;

    @Provides
    @Singleton
    public ConsumerConfig ConsumerConfig() {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list", config.getString("kafka.broker.list"));
        props.setProperty("zookeeper.connect", config.getString("kafka.zookeeperConnect"));
        props.setProperty("zookeeper.session.timeout.ms", "30000");
        props.setProperty("zookeeper.connection.timeout.ms", "30000");
        props.put("topic", config.getString("kafka.inputTopic"));
        props.put("offsets.storage", "kafka");
        props.put("auto.offset.reset", "smallest");
        props.put("retries", "3");
        props.put("group.id", config.getString("kafka.inputGroupId"));
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.timeout.ms", "-1");
        props.put("rebalance.max.retries", "10");
        props.put("rebalance.backoff.ms", "5000");
        props.put("fetch.message.max.bytes", String.valueOf(1024 * 1024));
        return new ConsumerConfig(props);
    }

    @Provides
    @Singleton
    public HConnection createHbaseConnection() {
        Configuration hBaseConf = HBaseConfiguration.create();
        hBaseConf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"));
        hBaseConf.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zookeeper.property.clientPort"));

        try {
            return HConnectionManager.createConnection(hBaseConf);
        } catch (ZooKeeperConnectionException var5) {
            log.error("Error creating HBase connection", var5);
            throw new RuntimeException("Unexpected exception obtaining connection to HBase", var5);
        }
    }


    @Override
    public void configure() {
        bind(UserEventHbaseWriter.class).toInstance(new UserEventHbaseWriterImpl());
    }


}
