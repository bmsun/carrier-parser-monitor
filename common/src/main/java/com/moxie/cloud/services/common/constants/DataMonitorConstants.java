package com.moxie.cloud.services.common.constants;

/**
 * Created by zhanghesheng on 2017/6/11.
 */
public class DataMonitorConstants {

    /**======================  BILL ====================== */

    public static final String BILL_BLANK_RESULT = "账单信息为空";

    public static final String BILL_CRAWL_ERROR = "账单信息采集失败";

    public static final String BILL_MONTH_EXTRACT_ERROR = "账单信息解析异常";

    public static final String BILL_BASE_FEE_EXTRACT_ERROR = "基本费用解析为0或负值";

    public static final String BILL_TOTAL_FEE_EXTRACT_ERROR = "账单各项费用合计与总额不等";

    public static final String BILL_DATE_EXTRACT_ERROR = "账单日期解析异常";

    public static final String BILL_ENDDATE_BEFORE_START_ERROR = "账单起始日期晚于截止日期";

    public static final String BILL_ENDDATE_EQUAL_START_ERROR = "账单起始日期与截止日期一致";

    public static final String BILL_STARTDATE_NOT_FIRSTDAY_ERROR = "账单起始日期不为1号";


    /***======================  PEOPLE ====================== */
    public static final String PEOPLE_NAME_BLANK = "姓名为空";
    public static final String PEOPLE_NAME_INVALID = "姓名不合法";
    public static final String PEOPLE_IDCARD_BLANK = "身份证件信息缺失";
    public static final String PEOPLE_IDCARD_INVALID = "身份证件不合法";
    public static final String PEOPLE_REALNAME_STATUS_BLANK = "实名制状态为空";

    /***======================  Mobile ====================== */
    public static final String MOBILE_BLANK = "手机信息为空";
    public static final String MOBILE_OPEN_TIME_BLANK = "入网时间为空";
    public static final String MOBILE_OPEN_TIME_INVALID = "入网时间解析异常";
    public static final String MOBILE_STAR_BLANK = "星级为空";
    public static final String MOBILE_STAR_INVALID = "星级解析异常";
    public static final String MOBILE_STATE_BLANK = "状态为空";
    public static final String MOBILE_STATUS_INVALID = "状态解析异常";
    public static final String MOBILE_BALANCE_BLANK = "余额为空";
    public static final String MOBILE_PACKAGE_NAME_BLANK = "主套餐为空";

    /**======================  CALL ====================== */
    public static final String CALL_NO_DATA = "通话记录为空";
    public static final String CALL_CRAWL_ERROR = "通话详情采集失败";
    public static final String CALL_MONTH_NO_DATA = "通话详情单月份记录解析为空";

    public static final String CALL_DIAL_TYPE_BLANK = "语音通信类型为空";
    public static final String CALL_PEER_INVALID_NOT_ALL_NUM = "语音对方号码不是全数字";
    public static final String CALL_PEER_INVALID_SAME = "语音对方号码和主号码相同";
    public static final String CALL_PEER_BLANK = "语音对方号码为空";
    public static final String CALL_DURATION_BLANK = "通话时长为0";
    public static final String CALL_LOCATION_TYPE_BLANK = "通话地点类型为空";
    public static final String CALL_LOCATION_BLANK = "通话地点为空";
    public static final String CALL_TIME_AFTER_CURRENT = "通话开始时间迟于当前时间";

    /***======================  SMS ====================== */
    public static final String SMS_NO_DATA = "短信记录为空";
    public static final String SMS_CRAWL_ERROR = "短信详情采集失败";
    public static final String SMS_MONTH_NO_DATA = "短信详情单月份记录解析为空";
    public static final String SMS_DIAL_TYPE_BLANK = "短信发送类型为空";
    public static final String SMS_PEER_INVALID_NOT_ALL_NUM = "短信对方号码不是全数字";
    public static final String SMS_PEER_INVALID_SAME = "短信对方号码和主号码相同";
    public static final String SMS_PEER_BLANK = "语音对方号码为空";
    public static final String SMS_LOCATION_BLANK = "通话地点为空";
    public static final String SMS_MSG_TYPE_BLANK = "业务类型为空";
    public static final String SMS_TIME_AFTER_CURRENT = "短信收/发时间晚于当前时间";
    public static final String SMS_SERVICENAME_BLANK = "业务名称为空";

    /***======================  FLOW ====================== */
    public static final String FLOW_NO_DATA = "流量记录为空";
    public static final String FLOW_CRAWL_ERROR = "流量详情采集失败";
    public static final String FLOW_MONTH_NO_DATA = "流量详情单月份记录解析为空";
    public static final String FLOW_USAGE_BLANK = "流量使用量为0";
    public static final String FLOW_NET_TYPE_BLANK = "流量网络类型为空";
    public static final String FLOW_DURATION_BLANK = "流量使用时长为0";
    public static final String FLOW_LOCATION_BLANK = "流量使用地为空";
    public static final String FLOW_SERVICENAME_BLANK = "上网业务名称为空";

    /***======================  Package ====================== */
    public static final String PACKAGE_NO_DATA = "套餐信息为空";
    public static final String PACKAGE_CRAWL_ERROR = "套餐信息采集失败";
    public static final String PACKAGE_MONTH_NO_DATA = "套餐信息单月份记录解析为空";
    public static final String PACKAGE_USAGE_INVALID = "套餐使用量解析异常";
    public static final String PACKAGE_UNIT_BLANK = "套餐单位为空";
    public static final String PACKAGE_ENDDATE_START_SAME = "套餐信息起始日期与截止日期一致";
    public static final String PACKAGE_ENDDATE_BEFORE_START_ERROR = "套餐信息起始日期大于截止日期";
    public static final String PACKAGE_DATE_NOT_FIRSTDAY_ERROR = "套餐信息起始日期不为1号";
    public static final String PACKAGE_DATE_EXTRACT_ERROR = "套餐信息起止日期解析异常";

    /***======================  Recharge ====================== */
    public static final String RECHARGE_NO_DATA = "充值信息为空";
    public static final String RECHARGE_EXTRACT_INVALID = "充值解析异常";
    public static final String RECHARGE_MONEY_EXTRACT_INVALID = "充值金额解析异常";

}

