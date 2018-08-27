package com.moxie.cloud.services.server.config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;


@Data
@Component
public class KafkaProperties {

    private final Integer MESSAGE_MAX_BYTES = 10000000;

    @Autowired
    private Environment env;

    public String kafkaServers() {
        return this.env.getProperty("base.kafka.servers");
    }

    public String kafkaAcks() {
        return this.env.getProperty("base.kafka.producer.acks");
    }

    public Long kafkaBuffer() {
        return this.env.getProperty("base.kafka.producer.buffer", Long.class, null);
    }

    public String kafkaCompressionType() {
        return this.env.getProperty("base.kafka.producer.compression");
    }

    public Integer kafkaRetryTimes() {
        return this.env.getProperty("base.kafka.producer.retry", Integer.class, null);

    }

    public Long kafkaLinger() {
        return this.env.getProperty("base.kafka.producer.linger.ms", Long.class, 5L);
    }

    public Integer kafkaMaxSendSize() {
        return this.env.getProperty("base.kafka.producer.max.request.size", Integer.class, MESSAGE_MAX_BYTES);
    }

    public Integer kafkaMaxBlockMs() {
        return this.env.getProperty("base.kafka.producer.max.block.ms", Integer.class, null);
    }

    /**
     * 消费者是否自动提交消费位移，默认为true。
     * 如果需要减少重复消费或者数据丢失，你可以设置为false。
     * 如果为true，你可能需要关注自动提交的时间间隔，该间隔由auto.commit.interval.ms设置。
     * @return
     */
    public Boolean kafkaAutoCommit() {
        return this.env.getProperty("base.kafka.consumer.enable.auto.commit", Boolean.class, null);
    }

    /**
     * 消费者指定从broker读取消息时最小的数据量
     * 当消费者从broker读取消息时，如果数据量小于这个阈值，broker会等待直到有足够的数据，然后才返回给消费者
     * @return
     */
    public Integer kafkaMinFetchSize() {
        return this.env.getProperty("base.kafka.consumer.fetch.min.bytes", Integer.class, 1000);
    }

    /**
     * 每个分区返回的最多字节数
     * 注意：max.partition.fetch.bytes必须要比broker能够接收的最大的消息（由max.message.size设置）大，否则会导致消费者消费不了消息
     * @return
     */
    public Integer kafkaMaxFetchSize() {
        return this.env.getProperty("base.kafka.consumer.max.partition.fetch.bytes", Integer.class, MESSAGE_MAX_BYTES);
    }

    /**
     * 消费者归属的消费组
     * @return
     */
    public String kafkaConsumerGroup() {
        return this.env.getProperty("base.kafka.consumer.group");
    }

    /**
     * 消费的topic
     * @return
     */
    public List<String> kafkaConsumerTopics() {
        String topics = this.env.getProperty("base.kafka.consumer.topics");
        return StringUtils.isBlank(topics) ? null : Arrays.asList(topics.split(","));
    }

    /**
     * 消费者会话过期时间，kafka默认10秒
     * @return
     */
    public Integer kafkaSessionTimeoutMs() {
        return this.env.getProperty("base.kafka.consumer.session.timeout.ms", Integer.class, 10000);
    }

    /**
     * 控制一个poll()调用返回的记录数，可以用来控制应用在拉取循环中的处理数据量。
     * @return
     */
    public Integer kafkaMaxPollRecords() {
        return this.env.getProperty("base.kafka.consumer.max.poll.records", Integer.class, 100);
    }
}
