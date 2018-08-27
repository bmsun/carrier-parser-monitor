package com.moxie.cloud.services.common.dto;

import lombok.Data;

/**
 * Created by zhanghesheng on 2017/8/15.
 */
@Data
public class CarrierVerifty {
    private Long id;
    private String carrier;
    private String province;
    private String crawlChannel;
    private String userName;
    private String idCard;
    private String realNameStatus;
    private String openTime;
    private String starLevel;
    private String packageName;
    private String availableBalance;
    private String state;
    private String msgLocation;
    private String sendType;
    private String msgType;
    private String serviceName;
    private String location;
    private String locationType;
    private String flowDurationInSecond;
    private String netType;
    private String flowServiceName;
    private String flowLocation;

}
