package com.moxie.cloud.services.common.enums;

import lombok.Getter;
import lombok.ToString;

/**
 * 数据类型
 */
@Getter
@ToString
public enum DataTypeEnum {

    EMPTY("未转换类型"),
    BASIC("基础信息"),
    PACKAGE_USAGE("套餐信息"),
    BILL("账单信息"),
    CALL("通话详情"),
    SMS("短信详情"),
    NET("流量详情"),
    RECHARGE("充值信息"),
    FAMILY("亲情网信息"),
    SURF("上网记录");

    private final String canonicalName;

    DataTypeEnum(String name) {
        this.canonicalName = name;
    }
}
