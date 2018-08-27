package com.moxie.cloud.services.server.kafka;

import com.google.common.collect.ImmutableList;
import com.moxie.cloud.services.common.enums.TaskTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Service
@Slf4j
public class KafkaService {

    @Autowired(required = false)
    private KafkaConsumer<String, String> consumer;

    @Autowired(required = false)
    private KafkaProducer<String, String> producer;

    public KafkaService() {
    }

    public void subscribe(ImmutableList<String> topics) {
        this.consumer.subscribe(topics);
    }

    public List<String> poll(Long timeout) {
        return this.poll((KafkaConsumer) null, timeout);
    }

    /**
     * timeout为Long.MAX_VALUE,意味着消费者会无限制地阻塞,直到有下一条记录返回的时候.
     * 这时如果使用标志位也是无法退出循环的,所以只能由触发关闭的线程调用consumer.wakeup来中断进行中的poll,
     * 这个调用会导致抛出WakeupException
     *
     * @param kafkaConsumer
     * @param timeout
     * @return
     */
    public List<String> poll(KafkaConsumer<String, String> kafkaConsumer, Long timeout) {
        List<String> messageList = new ArrayList<>();
        if (kafkaConsumer == null) {
            kafkaConsumer = this.consumer;
        }
        ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
        if (records != null) {
            records.forEach(record -> {
                String  value = record.value();
                if (StringUtils.isNotBlank(value)) {
                    messageList.add(value);
                }
            });
        }
        return messageList;
    }

    public void send(String topic, String key, String value) {
        this.send(new ProducerRecord(topic, key, value));
    }

    private void send(ProducerRecord record) {
        Future future = this.producer.send(record);
        try {
            if (future != null) {
                if (future.get() != null) {
                    if (future.get() instanceof ExecutionException) {
                        throw new RuntimeException((ExecutionException) future.get());
                    }
                }
            }
        } catch (Throwable var11) {
            throw new RuntimeException(var11);
        }
    }

    public void commit() {
        try {
            this.consumer.commitSync();
        } catch (Exception e) {
            log.error("action=consume, kafka consume message, sync commit offset occur exception.", e);

        }

    }

    public void commit(KafkaConsumer kafkaConsumer) {
        kafkaConsumer.commitSync();
    }

    @PreDestroy
    public void destroy() {
        if (this.consumer != null) {
            consumer.wakeup();
        }
    }

    /**
     * 关闭消费者
     */
    public void closeConsumer() {
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

}
