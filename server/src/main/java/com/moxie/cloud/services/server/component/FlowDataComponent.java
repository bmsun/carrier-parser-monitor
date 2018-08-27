package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.moxie.cloud.carrier.entity.MobileNetFlowEntity;
import com.moxie.cloud.carrier.entity.ShortMessageEntity;
import com.moxie.cloud.service.common.dto.MoxieApiErrorMessage;
import com.moxie.cloud.service.common.exception.MoxieApiException;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.BillModel;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("flowDataComponent")
public class FlowDataComponent extends BaseComponent {


    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage flowMessage = copyParserMonitorMessage(monitorMessage);
        flowMessage.setType(this.getType());
        Map<String, List<String>> flowCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (flowCrawlerMap == null || flowCrawlerMap.isEmpty()) return;
        verifyflows(flowCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, flowMessage);

    }

    private void verifyflows(Map<String, List<String>> flowCrawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage flowMessage) {

        List<BillModel> billModelList = mobileDetailInfo.getBillModelList();
        List<MobileNetFlowEntity> flowEntityList = Lists.newArrayList();
        if (billModelList != null) {
            for (BillModel billModel : billModelList) {
                List<MobileNetFlowEntity> flowList = billModel.getNetFlowList();
                if (flowList != null && !flowList.isEmpty()) {
                    flowEntityList.addAll(flowList);
                }
            }
        }
        Map<String, Long> flowCountByMonth = groupByGetTotal(flowEntityList);
        if (flowCountByMonth.size() == 0) {
            //流量记录未采集到或未解析成功
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_NO_DATA), DataMonitorConstants.FLOW_NO_DATA);
            ErrorMessage errorMessage = new ErrorMessage();
            errorMessage.setMessage(DataMonitorConstants.FLOW_NO_DATA);
            List<ErrorMessage> errorMessageList = Lists.newArrayList();
            errorMessageList.add(errorMessage);
            buildMessage(flowCrawlerMap.size(), flowMessage, CodeEnum.CODE_NO_DATA, errorMessageList);
            //入库
            addMessageToDb(flowMessage);
            return;
        } else {
            Pair<Integer, Integer> pair = checkData(flowCrawlerMap, flowCountByMonth);
            int crawlErrorNum = pair.getLeft();
            int parserErrorNum = pair.getRight();
            if (crawlErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_CRAWL_ERROR), DataMonitorConstants.FLOW_CRAWL_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(flowMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.FLOW_CRAWL_ERROR);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(crawlErrorNum, monitorMessage, CodeEnum.CODE_CRAWL_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
            if (parserErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_MONTH_NO_DATA), DataMonitorConstants.FLOW_MONTH_NO_DATA);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(flowMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.FLOW_MONTH_NO_DATA);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(parserErrorNum, monitorMessage, CodeEnum.CODE_PARSER_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
        }

        verifyFlowField(carrierVerify, keyPre, flowEntityList, flowMessage);
    }

    private void verifyFlowField(CarrierVerifty carrierVerify, String keyPre, List<MobileNetFlowEntity> flowEntityList, ParserMonitorMessage flowMessage) {
        if (!flowEntityList.isEmpty()) {
            int flowDurationNum = 0;
            int netTypeNum = 0;
            int flowUsedNum = 0;
            int locationNum = 0;
            int serviceNameNum = 0;
            for (MobileNetFlowEntity flowEntity : flowEntityList) {
                    //流量使用时长
                    if (isTrue(carrierVerify.getFlowDurationInSecond())
                            && (flowEntity.getDurationInSecond() == null || flowEntity.getDurationInSecond() == 0)) {
                        flowDurationNum++;
                    }
                    //流量网络类型为空
                    if (isTrue(carrierVerify.getNetType()) && StringUtils.isBlank(flowEntity.getNetType())) {
                        netTypeNum++;
                    }
                    //使用量为0
                    if (flowEntity.getDurationInFlow() == null || flowEntity.getDurationInFlow() == 0) {
                        flowUsedNum++;
                    }
                    //上网地
                    if (isTrue(carrierVerify.getFlowLocation()) && StringUtils.isBlank(flowEntity.getLocation())) {
                        locationNum++;
                    }
                    //上网业务名称
                    if (isTrue(carrierVerify.getFlowServiceName()) && StringUtils.isBlank(flowEntity.getServiceName())) {
                        serviceNameNum++;
                    }
            }

            List<ErrorMessage> messageList = Lists.newArrayList();
            if (flowDurationNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_DURATION_BLANK), DataMonitorConstants.FLOW_DURATION_BLANK);
                addErrorMsg("durationInSecond", DataMonitorConstants.FLOW_DURATION_BLANK, messageList, flowDurationNum, flowEntityList.size());
            }
            if (netTypeNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_NET_TYPE_BLANK), DataMonitorConstants.FLOW_NET_TYPE_BLANK);
                addErrorMsg("netType", DataMonitorConstants.FLOW_NET_TYPE_BLANK, messageList, netTypeNum, flowEntityList.size());
            }
            if (flowUsedNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_USAGE_BLANK), DataMonitorConstants.FLOW_USAGE_BLANK);
                addErrorMsg("durationInFlow", DataMonitorConstants.FLOW_USAGE_BLANK, messageList, flowUsedNum, flowEntityList.size());
            }
            if (locationNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_LOCATION_BLANK), DataMonitorConstants.FLOW_LOCATION_BLANK);
                addErrorMsg("flowLocation", DataMonitorConstants.FLOW_LOCATION_BLANK, messageList, locationNum, flowEntityList.size());
            }
            if (serviceNameNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.FLOW_SERVICENAME_BLANK), DataMonitorConstants.FLOW_SERVICENAME_BLANK);
                addErrorMsg("serviceName", DataMonitorConstants.FLOW_SERVICENAME_BLANK, messageList, serviceNameNum, flowEntityList.size());
            }

            if (messageList.size()>0){
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(flowMessage);
                buildMessage(0, monitorMessage, CodeEnum.CODE_DATA_INVALIDE, messageList);
                addMessageToDb(monitorMessage);
            }
        }

    }

    @Override
    public String getType() {
        return DataTypeEnum.NET.name();
    }


    //按月份分组，并得到每个月总数据数totalNum
    private Map<String, Long> groupByGetTotal(List<MobileNetFlowEntity> flowEntities) {
        Map<String, Long> results = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(flowEntities)) {
            results = flowEntities
                    .parallelStream()
                    .collect(Collectors.groupingBy(flow -> formatMonth(flow.getTime()), Collectors.counting()));
        }
        return results;
    }
}
