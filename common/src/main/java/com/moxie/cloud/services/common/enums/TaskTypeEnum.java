package com.moxie.cloud.services.common.enums;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

@Getter
public enum TaskTypeEnum {
   PARSER_MONITOR("parser-monitor", "运营商实时监控"),
    NULL("null", "未知类型");

    //common-info-monitor,call-info-monitor,sms-info-monitor,flow-info-monitor

//    COMMON_INFO_MONITOR("common-info-monitor", "运营商基础信息实时监控"),
//    CALL_INFO_MONITOR("call-info-monitor", "运营商通话信息实时监控"),
//    SMS_INFO_MONITOR("sms-info-monitor", "运营商短信信息实时监控"),
//    FLOW_INFO_MONITOR("flow-info-monitor", "运营商流量信息实时监控"),
//

    private String type;

    private String desc;

    TaskTypeEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public static TaskTypeEnum getByType(String type) {
        if (StringUtils.isBlank(type)) {
            return NULL;
        }
        Optional<TaskTypeEnum> optional = Arrays.stream(values())
                .filter(v -> StringUtils.equalsIgnoreCase(v.getType(), type))
                .findFirst();
        return optional.isPresent() ? optional.get() : NULL;
    }
}
