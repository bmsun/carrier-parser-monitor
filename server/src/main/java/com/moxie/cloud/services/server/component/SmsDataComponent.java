package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.moxie.cloud.carrier.entity.ShortMessageEntity;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.BillModel;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("smsDataComponent")
@Slf4j
public  class SmsDataComponent extends BaseComponent{

    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage smsMessage = copyParserMonitorMessage(monitorMessage);
        smsMessage.setType(this.getType());
        Map<String, List<String>> smsCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (smsCrawlerMap == null||smsCrawlerMap.isEmpty()) return;
        verifySmses(smsCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, smsMessage);

    }
    private void verifySmses(Map<String, List<String>> crawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage message) {
        List<BillModel> billModelList = mobileDetailInfo.getBillModelList();
        List<ShortMessageEntity> smsList = Lists.newArrayList();
        if (billModelList != null) {
            for (BillModel billModel : billModelList) {
                List<ShortMessageEntity> shortMessageList = billModel.getShortMessageList();
                if (shortMessageList != null && !shortMessageList.isEmpty()) {
                    smsList.addAll(shortMessageList);
                }
            }
        }
        Map<String, Long> smsCountByMonth = groupByGetTotal(smsList);
        if (smsCountByMonth.size() == 0) {
            //短信记录未采集到或未解析成功
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_NO_DATA), DataMonitorConstants.SMS_NO_DATA);
            ErrorMessage errorMessage = new ErrorMessage();
            errorMessage.setMessage(DataMonitorConstants.SMS_NO_DATA);
            List<ErrorMessage> errorMessageList = Lists.newArrayList();
            errorMessageList.add(errorMessage);
            buildMessage(crawlerMap.size(), message, CodeEnum.CODE_NO_DATA, errorMessageList);
            //入库
            addMessageToDb(message);
            return;
        } else {

            Pair<Integer, Integer> pair = checkData(crawlerMap, smsCountByMonth);
            int crawlErrorNum = pair.getLeft();
            int parserErrorNum = pair.getRight();
            if (crawlErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_CRAWL_ERROR), DataMonitorConstants.SMS_CRAWL_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.SMS_CRAWL_ERROR);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(crawlErrorNum, monitorMessage, CodeEnum.CODE_CRAWL_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
            if (parserErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_MONTH_NO_DATA), DataMonitorConstants.SMS_MONTH_NO_DATA);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.SMS_MONTH_NO_DATA);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(parserErrorNum, monitorMessage, CodeEnum.CODE_PARSER_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
        }

        verifySmsField(carrierVerify, keyPre, smsList, message);
    }


    //通话每条记录:具体字段校验
    private void verifySmsField(CarrierVerifty carrierVerify, String keyPre, List<ShortMessageEntity> smsList, ParserMonitorMessage message) {
        if (!smsList.isEmpty()) {
            List<ErrorMessage> messageList = Lists.newArrayList();
            int sendTypeNum = 0;
            int peerNumberNullNum = 0;
            int peerNumberSameNum = 0;
            int peerNumberErrorNum = 0;
            int locationNum = 0;
            int msgTypeNum = 0;
            int serviceNameNum = 0;
            int smsTimeNum = 0;
            for (ShortMessageEntity smsEntity : smsList) {
                //规则
                //发送类型不能为空
                if(isTrue(carrierVerify.getSendType())){
                    if (StringUtils.isBlank(smsEntity.getSendType())) {
                        sendTypeNum++;
                    }
                }

                //对方号码不能为空;不能包含非数字;不能与本手机号码相同
                if (StringUtils.isBlank(smsEntity.getPeerNumber())) {
                    peerNumberNullNum++;
                } else if (smsEntity.getPeerNumber().equals(smsEntity.getMobile())) {
                    peerNumberSameNum++;
                } else if (!IS_NUMBER.matcher(smsEntity.getPeerNumber()).matches()) {
                    peerNumberErrorNum++;
                }
                //通话地点不能为空
                if(isTrue(carrierVerify.getMsgLocation())){
                    if (StringUtils.isBlank(smsEntity.getLocation())) {
                        locationNum++;
                    }
                }
                //业务类型不能为空
                if(isTrue(carrierVerify.getMsgType())){
                    if (StringUtils.isBlank(smsEntity.getMsgType())) {
                        msgTypeNum++;
                    }
                }

                //业务名称为空
                if(isTrue(carrierVerify.getServiceName())){
                    if (StringUtils.isBlank(smsEntity.getServiceName())) {
                        serviceNameNum++;
                    }
                }

                //通话日期超过当前时间
                if (smsEntity.getTime().after(new Date())) {
                    smsTimeNum++;
                }
            }


            if (sendTypeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_DIAL_TYPE_BLANK), DataMonitorConstants.SMS_DIAL_TYPE_BLANK);
                addErrorMsg("sendType", DataMonitorConstants.SMS_DIAL_TYPE_BLANK, messageList, sendTypeNum, smsList.size());
            }
            //
            if (peerNumberNullNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_PEER_BLANK), DataMonitorConstants.SMS_PEER_BLANK);
                addErrorMsg("peerNumber", DataMonitorConstants.SMS_PEER_BLANK, messageList, peerNumberNullNum, smsList.size());
            }
            //
            if (peerNumberErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_PEER_INVALID_NOT_ALL_NUM), DataMonitorConstants.SMS_PEER_INVALID_NOT_ALL_NUM);
                addErrorMsg("peerNumber", DataMonitorConstants.SMS_PEER_INVALID_NOT_ALL_NUM, messageList, peerNumberErrorNum, smsList.size());
            }
            //
            if (peerNumberSameNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_PEER_INVALID_SAME), DataMonitorConstants.SMS_PEER_INVALID_SAME);
                addErrorMsg("peerNumber", DataMonitorConstants.SMS_PEER_INVALID_SAME, messageList, peerNumberSameNum, smsList.size());
            }
            //
            if (locationNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_LOCATION_BLANK), DataMonitorConstants.SMS_LOCATION_BLANK);
                addErrorMsg("location", DataMonitorConstants.SMS_LOCATION_BLANK, messageList, locationNum, smsList.size());
            }
            if (msgTypeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_MSG_TYPE_BLANK), DataMonitorConstants.SMS_MSG_TYPE_BLANK);
                addErrorMsg("msgType", DataMonitorConstants.SMS_MSG_TYPE_BLANK, messageList, msgTypeNum, smsList.size());
            }
            if (serviceNameNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_SERVICENAME_BLANK), DataMonitorConstants.SMS_SERVICENAME_BLANK);
                addErrorMsg("serviceName", DataMonitorConstants.SMS_SERVICENAME_BLANK, messageList, serviceNameNum, smsList.size());
            }
            if (smsTimeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.SMS_TIME_AFTER_CURRENT), DataMonitorConstants.SMS_TIME_AFTER_CURRENT);
                addErrorMsg("smsTime", DataMonitorConstants.SMS_TIME_AFTER_CURRENT, messageList, smsTimeNum, smsList.size());
            }
            //入库
            if (messageList.size() > 0) {
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
                buildMessage(0, monitorMessage, CodeEnum.CODE_DATA_INVALIDE, messageList);
                addMessageToDb(monitorMessage);
            }
        }
    }

    @Override
    public String getType() {
        return DataTypeEnum.SMS.name();
    }


    //按月份分组，并得到每个月总数据数totalNum
    private Map<String, Long> groupByGetTotal(List<ShortMessageEntity> shortMessageEntities) {
        Map<String, Long> results = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(shortMessageEntities)) {
            results = shortMessageEntities
                    .parallelStream()
                    .collect(Collectors.groupingBy(sms -> formatMonth(sms.getTime()), Collectors.counting()));
        }
        return results;
    }
}
