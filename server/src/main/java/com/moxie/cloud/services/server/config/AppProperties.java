package com.moxie.cloud.services.server.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;


@Data
@Component
public class AppProperties {

    /**
     * 核心线程数
     */
    @Value("${carrier.parser.monitor.corePoolSize:20}")
    @NotNull
    private Integer corePoolSize;

    /**
     * 无任务休眠时间，单位毫秒
     */
    @Value("${carrier.parser.monitor.noTaskSleepMilliSec:1000}")
    @NotNull
    private Long noTaskSleepMilliSec;


    /**
     * 队列最多等待的任务数
     */
    @Value("${carrier.parser.monitor.waitQueueMaxTaskNum:100}")
    @NotNull
    private Integer waitQueueMaxTaskNum;

    /**
     * 队列中等待任务数达到最大休眠的时间
     */
    @Value("${carrier.parser.monitor.waitQueueSleepMilliSec:1000}")
    @NotNull
    private Long waitQueueSleepMilliSec;

    @Value("${carrier.parser.msgSendClient.msgSendTag}")
    @NotNull
    private String msgSendTag;

    @Value("${carrier.parser.msgSendClient.webhookToken}")
    @NotNull
    private String webhookToken;


}
