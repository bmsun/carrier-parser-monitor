package com.moxie.cloud.services.common.dto;

import lombok.Data;

import java.math.BigInteger;
import java.util.Date;

@Data
public class CrawlMetadataEntity {
    private BigInteger id;
    private String tenantId;
    private String taskId;
    private String mobile;
    private String carrier;
    private String province;
    private Integer status;
    private String basic;
    private String bill;
    private String packageUsage;
    private String call;
    private String sms;
    private String recharge;
    private String net;
    private String family;
    private String reserved;
    private Date createTime;
    private String surf;
}
