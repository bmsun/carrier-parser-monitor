package com.moxie.cloud.services.server.kafka;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;

@Service
@Slf4j
public class KafkaHandler {

    @Autowired
    private ConsumerHandler consumerHandler;

    /**
     * 开始消费kafka中消息
     */
    public void startConsuming() {
        log.info("action=startConsuming, kafka consumer start comsuming subscribe message... ");
        consumerHandler.consume();
    }

    @PreDestroy
    public void shutdown(){
        log.info("action=shutdown, kafka consumer pre to close");
        consumerHandler.shutdown();
    }
}
