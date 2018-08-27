package com.moxie.cloud.services.common.dto;

import lombok.Data;

/**
 * kafka 发送的消息实体类
 * */
@Data
public class ParserMonitorInfo {
   private String crawlChannel;

   private CrawlMetadataEntity crawlMetadataEntity;

   private MobileDetailInfo mobileDetailInfo;
}
