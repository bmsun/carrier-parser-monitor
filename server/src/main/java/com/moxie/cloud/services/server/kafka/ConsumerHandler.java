package com.moxie.cloud.services.server.kafka;

import com.google.common.collect.ImmutableList;

import com.moxie.cloud.services.common.dto.MessageInfo;
import com.moxie.cloud.services.common.dto.ParserMonitorInfo;
import com.moxie.cloud.services.common.utils.BaseJsonUtils;
import com.moxie.cloud.services.common.utils.ZipUtils;
import com.moxie.cloud.services.server.config.AppProperties;
import com.moxie.cloud.services.server.config.KafkaProperties;
import com.moxie.cloud.services.server.process.TaskConsumerThreadPool;
import com.moxie.commons.MoxieBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Component
public class ConsumerHandler {

    @Autowired
    private KafkaHandler kafkaHandler;
    @Autowired
    private  KafkaService kafkaService;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private AppProperties appProperties;

    @Autowired
    private TaskConsumerThreadPool taskConsumerThreadPool;

//    @Autowired
//    private MonitorTaskStatusDao monitorTaskStatusDao;


    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * kafka消费者开始消费订阅的信息
     */
    public void consume() {
        kafkaService.subscribe(ImmutableList.copyOf(kafkaProperties.kafkaConsumerTopics()));
        List<ParserMonitorInfo> messageRecordList = new ArrayList<>();
        try {
            while(true) {
                if (closed.get()) {
                    kafkaService.destroy();
                }
                int waitingQueueSize = taskConsumerThreadPool.getWaitingQueueSize();
                if (waitingQueueSize >= appProperties.getWaitQueueMaxTaskNum()) {
                    log.error("action=consume, the number of the queue to wait for process has reached max limit. waitingQueueSize:{}, running task num is :{}",
                            waitingQueueSize, taskConsumerThreadPool.getSize());
                    try {
                        TimeUnit.MILLISECONDS.sleep(appProperties.getWaitQueueSleepMilliSec());
                    } catch (InterruptedException e) {
                        log.error("action=consume, 等待队列的任务数达到上限，休眠异常", e);
                    }
                }

                List<String> messageList = kafkaService.poll(100L*20);

                if (messageList == null || messageList.isEmpty()) {
                    log.debug("action=consume, kafka consume message list is empty");
                    continue;
                }
                // step-1 获取所有接受到的信息
                messageList.stream().forEach(item -> {
                    MessageInfo messageInfo = BaseJsonUtils.readValue(item, MessageInfo.class);
                    if (messageInfo!=null){
                        ParserMonitorInfo parserMonitorInfo = null;
                        if (messageInfo.getFlag() == 0) {
                             parserMonitorInfo = MoxieBeanUtils.readValueSave(messageInfo.getParserMonitorInfo(), ParserMonitorInfo.class);
                        } else if (messageInfo.getFlag() == 1) {
                            parserMonitorInfo = MoxieBeanUtils.readValueSave(ZipUtils.gunzip(messageInfo.getParserMonitorInfo()), ParserMonitorInfo.class);
                        }
                        if (parserMonitorInfo != null) {
                            messageRecordList.add(parserMonitorInfo);
                        }
                    }
                });

                // step-2 任务给消息处理器处理
                taskConsumerThreadPool.batchAddTask(messageRecordList);
                messageRecordList.clear();
                // step-3 提交offset
                    //auto.commit=false时,需手动提交
                kafkaService.commit();

            }
        } catch (WakeupException wakeupException) {
            // ignore for shutdown
        } catch (Exception e) {
            log.error("action=consume, kafka consume message occur exception", e);
        } finally {
            kafkaService.closeConsumer();
        }
    }

    /**
     * 关闭kafka消费者
     */
    public void shutdown() {
        log.info("action=shutdown Kafka producer and consumer start to destroy before application shutdown");
        kafkaService.destroy();
    }

}
