package com.moxie.cloud.services.server.config;

import com.google.common.base.Preconditions;
import com.moxie.cloud.services.common.config.RedisConfiguration;
import com.moxie.cloud.services.server.kafka.KafkaService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

@Configuration
@ConditionalOnProperty({"base.kafka.enabled"})
@Import({KafkaService.class, RedisConfiguration.class})
public class KafkaConfig {

    @Autowired(
            required = false
    )
    private KafkaProperties kafkaProperties;

    @Value("${moxie.cloud.service.name:carrier-parser-monitor}")
    private String serviceName;

    public KafkaConfig() {
    }

    @PostConstruct
    public void init() {
        Preconditions.checkState(StringUtils.isNotBlank(this.kafkaProperties.kafkaServers()), "未找到配置项:base.kafka.servers");
    }

    @Bean
    public KafkaProducer<String, String> producer() {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", this.kafkaProperties.kafkaServers());
        if (StringUtils.isNotBlank(this.kafkaProperties.kafkaAcks())) {
            producerProps.put("acks", this.kafkaProperties.kafkaAcks());
        }

        if (this.kafkaProperties.kafkaBuffer() != null) {
            producerProps.put("buffer.memory", this.kafkaProperties.kafkaBuffer());
        }

        if (StringUtils.isNotBlank(this.kafkaProperties.kafkaCompressionType())) {
            producerProps.put("compression.type", this.kafkaProperties.kafkaCompressionType());
        }

        if (this.kafkaProperties.kafkaRetryTimes() != null) {
            producerProps.put("retries", this.kafkaProperties.kafkaRetryTimes());
            if (this.kafkaProperties.kafkaRetryTimes().intValue() != 0) {
                producerProps.put("max.in.flight.requests.per.connection", Integer.valueOf(1));
            }
        }

        producerProps.put("client.id", this.serviceName);
        if (this.kafkaProperties.kafkaLinger() != null) {
            producerProps.put("linger.ms", this.kafkaProperties.kafkaLinger());
        }

        if (this.kafkaProperties.kafkaMaxSendSize() != null) {
            producerProps.put("max.request.size", this.kafkaProperties.kafkaMaxSendSize());
        }

        if (this.kafkaProperties.kafkaMaxBlockMs() != null) {
            producerProps.put("max.block.ms", this.kafkaProperties.kafkaMaxBlockMs());
        }

        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer(producerProps);
    }

    @Bean
    @ConditionalOnProperty({"base.kafka.consumer.group"})
    public KafkaConsumer<String, String> consumer() {
        return this.createConsumer(this.kafkaProperties.kafkaConsumerGroup(), this.kafkaProperties.kafkaConsumerTopics());
    }

    public KafkaConsumer<String, String> createConsumer(String groupId, List<String> topics) {
        Properties consumerProp = this.consumerProp();
        consumerProp.put("group.id", groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer(consumerProp);
        if (CollectionUtils.isNotEmpty(topics)) {
            consumer.subscribe(topics);
        }

        return consumer;
    }

    private Properties consumerProp() {
        Properties consumerProp = new Properties();
        consumerProp.put("bootstrap.servers", this.kafkaProperties.kafkaServers());
        if (this.kafkaProperties.kafkaAutoCommit() != null) {
            consumerProp.put("enable.auto.commit", this.kafkaProperties.kafkaAutoCommit());
        }

        if (this.kafkaProperties.kafkaMaxFetchSize() != null) {
            consumerProp.put("max.partition.fetch.bytes", this.kafkaProperties.kafkaMaxFetchSize());
            consumerProp.put("fetch.message.max.bytes",this.kafkaProperties.kafkaMaxFetchSize());

        }

        consumerProp.put("fetch.min.bytes", this.kafkaProperties.kafkaMinFetchSize());
        consumerProp.put("client.id", this.serviceName);
        consumerProp.put("session.timeout.ms", this.kafkaProperties.kafkaSessionTimeoutMs());
        consumerProp.put("max.poll.records", this.kafkaProperties.kafkaMaxPollRecords());
        consumerProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerProp;
    }
}
