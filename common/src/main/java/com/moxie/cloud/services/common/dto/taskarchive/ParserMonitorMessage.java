package com.moxie.cloud.services.common.dto.taskarchive;

import lombok.Data;

import java.math.BigInteger;
import java.util.Date;

@Data
public class ParserMonitorMessage {
    BigInteger id;
    String taskId;
    String carrier;
    String province;
    String channel;
    String mobile;
    String tenantId;
    String code;
    String type;
    String errorMsg;
    String reserved;
    int count;
    Date createTime;
    Date updateTime;


}
