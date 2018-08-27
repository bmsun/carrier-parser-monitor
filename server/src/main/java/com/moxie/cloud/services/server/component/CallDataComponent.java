package com.moxie.cloud.services.server.component;

import com.moxie.cloud.carrier.entity.VoiceCallEntity;
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
import org.testng.collections.Lists;
import org.testng.collections.Maps;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("callDataComponent")
@Slf4j
public class CallDataComponent extends BaseComponent {


    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage callMessage = copyParserMonitorMessage(monitorMessage);
        callMessage.setType(this.getType());
        Map<String, List<String>> callCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (callCrawlerMap == null||callCrawlerMap.isEmpty()) return;
        verifyCalls(callCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, callMessage);
    }

    private void verifyCalls(Map<String, List<String>> crawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage message) {
        List<BillModel> billModelList = mobileDetailInfo.getBillModelList();
        List<VoiceCallEntity> calllist = Lists.newArrayList();
        if (billModelList != null) {
            for (BillModel billModel : billModelList) {
                List<VoiceCallEntity> voiceCallList = billModel.getVoiceCallList();
                if (voiceCallList != null && !voiceCallList.isEmpty()) {
                    calllist.addAll(voiceCallList);
                }
            }
        }
        Map<String, Long> callCountByMonth = groupByGetTotal(calllist);
        if (callCountByMonth.size() == 0) {
            //通话记录未采集到或未解析成功
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_NO_DATA), DataMonitorConstants.CALL_NO_DATA);
            ErrorMessage errorMessage = new ErrorMessage();
            errorMessage.setMessage(DataMonitorConstants.CALL_NO_DATA);
            List<ErrorMessage> errorMessageList = Lists.newArrayList();
            errorMessageList.add(errorMessage);
            buildMessage(crawlerMap.size(), message, CodeEnum.CODE_NO_DATA, errorMessageList);
            //入库
            addMessageToDb(message);
            return;
        } else {
            Pair<Integer, Integer> resultPair = checkData(crawlerMap, callCountByMonth);
            int crawlErrorNum = resultPair.getLeft();
            int parserErrorNum = resultPair.getRight();
            if (crawlErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_CRAWL_ERROR), DataMonitorConstants.CALL_CRAWL_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.CALL_CRAWL_ERROR);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(crawlErrorNum, monitorMessage, CodeEnum.CODE_CRAWL_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
            if (parserErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_MONTH_NO_DATA), DataMonitorConstants.CALL_MONTH_NO_DATA);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.CALL_MONTH_NO_DATA);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(parserErrorNum, monitorMessage, CodeEnum.CODE_PARSER_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
        }

        verifyCallField(carrierVerify, keyPre, calllist, message);
    }


    //通话每条记录:具体字段校验
    private void verifyCallField(CarrierVerifty carrierVerify, String keyPre, List<VoiceCallEntity> calllist, ParserMonitorMessage message) {
        if (!calllist.isEmpty()) {
            List<ErrorMessage> messageList = Lists.newArrayList();
            int dialTypeNum = 0;
            int peerNumberNullNum = 0;
            int peerNumberSameNum = 0;
            int peerNumberErrorNum = 0;
            int locationNum = 0;
            int locationTypeNum = 0;
            int callDurationNum = 0;
            int callTimeNum = 0;
            for (VoiceCallEntity voiceCallEntity : calllist) {
                //通话方式不能为空
                if (StringUtils.isBlank(voiceCallEntity.getDialType())) {
                    dialTypeNum++;
                }
                //对方号码不能为空;不能包含非数字;不能与本手机号码相同
                if (StringUtils.isBlank(voiceCallEntity.getPeerNumber())) {
                    peerNumberNullNum++;
                } else if (voiceCallEntity.getPeerNumber().equals(voiceCallEntity.getMobile())) {
                    peerNumberSameNum++;
                } else if (!IS_NUMBER.matcher(voiceCallEntity.getPeerNumber()).matches()) {
                    peerNumberErrorNum++;
                    log.error("不合规则的号码为：" + voiceCallEntity.getPeerNumber() + "\t" + voiceCallEntity.getCreateTime());

                }
                //通话地点不能为空
                if (isTrue(carrierVerify.getLocation())) {
                    if (StringUtils.isBlank(voiceCallEntity.getLocation())) {
                        locationNum++;
                    }
                }

                //通话地类型不能为空
                if (isTrue(carrierVerify.getLocationType())) {
                    if (StringUtils.isBlank(voiceCallEntity.getLocationType())) {
                        locationTypeNum++;
                    }
                }
                //通话地时长不能为0
                if (voiceCallEntity.getDurationInSecond() == null || voiceCallEntity.getDurationInSecond() == 0) {
                    callDurationNum++;
                }
                //通话日期超过当前时间
                if (voiceCallEntity.getTime().after(new Date())) {
                    callTimeNum++;
                }
            }

            if (dialTypeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_DIAL_TYPE_BLANK), DataMonitorConstants.CALL_DIAL_TYPE_BLANK);
                addErrorMsg("dialType", DataMonitorConstants.CALL_DIAL_TYPE_BLANK, messageList, dialTypeNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_DIAL_TYPE_BLANK, dialTypeNum);
            }
            //
            if (peerNumberNullNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_PEER_BLANK), DataMonitorConstants.CALL_PEER_BLANK);
                addErrorMsg("peerNumber", DataMonitorConstants.CALL_PEER_BLANK, messageList, peerNumberNullNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_PEER_BLANK, peerNumberNullNum);
            }
            if (peerNumberSameNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_PEER_INVALID_SAME), DataMonitorConstants.CALL_PEER_INVALID_SAME);
                addErrorMsg("peerNumber", DataMonitorConstants.CALL_PEER_INVALID_SAME, messageList, peerNumberSameNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_PEER_INVALID_SAME, peerNumberSameNum);
            }
            if (peerNumberErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_PEER_INVALID_NOT_ALL_NUM), DataMonitorConstants.CALL_PEER_INVALID_NOT_ALL_NUM);
                addErrorMsg("peerNumber", DataMonitorConstants.CALL_PEER_INVALID_NOT_ALL_NUM, messageList, peerNumberErrorNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_PEER_INVALID_NOT_ALL_NUM, peerNumberErrorNum);
            }
            //
            if (locationNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_LOCATION_BLANK), DataMonitorConstants.CALL_LOCATION_BLANK);
                addErrorMsg("location", DataMonitorConstants.CALL_LOCATION_BLANK, messageList, locationNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_LOCATION_BLANK, locationNum);
            }
            if (locationTypeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_LOCATION_TYPE_BLANK), DataMonitorConstants.CALL_LOCATION_TYPE_BLANK);
                addErrorMsg("locationType", DataMonitorConstants.CALL_LOCATION_TYPE_BLANK, messageList, locationTypeNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_LOCATION_TYPE_BLANK, locationTypeNum);
            }
            if (callDurationNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_DURATION_BLANK), DataMonitorConstants.CALL_DURATION_BLANK);
                addErrorMsg("durationInSecond", DataMonitorConstants.CALL_DURATION_BLANK, messageList, callDurationNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_DURATION_BLANK, callDurationNum);
            }
            if (callTimeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.CALL_TIME_AFTER_CURRENT), DataMonitorConstants.CALL_TIME_AFTER_CURRENT);
                addErrorMsg("durationInSecond", DataMonitorConstants.CALL_TIME_AFTER_CURRENT, messageList, callTimeNum, calllist.size());
                log.debug("异常：[{}],条数为[{}]", DataMonitorConstants.CALL_TIME_AFTER_CURRENT, callTimeNum);
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
        return DataTypeEnum.CALL.name();
    }
    //按月份分组，并得到每个月总数据数totalNum
    private Map<String, Long> groupByGetTotal(List<VoiceCallEntity> voiceCallEntityList) {
        Map<String, Long> results = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(voiceCallEntityList)) {
            results = voiceCallEntityList
                    .parallelStream()
                    .collect(Collectors.groupingBy(voiceCallEntity -> formatMonth(voiceCallEntity.getTime()), Collectors.counting()));
        }
        return results;
    }
}
