package com.moxie.cloud.services.server.biz.impl;


import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.CrawlMetadataEntity;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.ParserMonitorInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.enums.TaskTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import com.moxie.cloud.services.common.utils.BaseJsonUtils;
import com.moxie.cloud.services.common.utils.JedisUtils;
import com.moxie.cloud.services.server.biz.MessageExecutor;
import com.moxie.cloud.services.server.component.*;
import com.moxie.cloud.services.server.service.CarrierVerifyService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.*;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


@Service
@Slf4j
public class MonitorMesageExcutor implements MessageExecutor {

    @Autowired
    CarrierVerifyService carrierVerifyService;

    private static  Map<String, BaseComponent> componentMap = new HashMap<>();

    @Autowired
    BillDataComponent billDataComponent;
    @Autowired
    CallDataComponent callDataComponent;
    @Autowired
    FlowDataComponent flowDataComponent;
    @Autowired
    PackageDataComponent packageDataComponent;
    @Autowired
    SmsDataComponent smsDataComponent;
    @Autowired
    BasicDataComponent basicDataComponent;
    @Autowired
    RechargeDataComponent rechargeDataComponent;

    @PostConstruct
    private void init() {
        componentMap.put(DataTypeEnum.BILL.name(), billDataComponent);
        componentMap.put(DataTypeEnum.CALL.name(), callDataComponent);
        componentMap.put(DataTypeEnum.NET.name(), flowDataComponent);
        componentMap.put(DataTypeEnum.PACKAGE_USAGE.name(), packageDataComponent);
        componentMap.put(DataTypeEnum.SMS.name(), smsDataComponent);
        componentMap.put(DataTypeEnum.BASIC.name(), basicDataComponent);
        componentMap.put(DataTypeEnum.RECHARGE.name(), rechargeDataComponent);

    }


    @Override
    public TaskTypeEnum getTaskType() {
        return TaskTypeEnum.PARSER_MONITOR;
    }


    @Override
    public void execute(Object message) throws Exception {
        ParserMonitorInfo parserMonitorInfo = (ParserMonitorInfo) message;
        CrawlMetadataEntity crawlMetadataEntity = parserMonitorInfo.getCrawlMetadataEntity();
        if (crawlMetadataEntity != null) {
            String crawlChannel = parserMonitorInfo.getCrawlChannel();
            String province = crawlMetadataEntity.getProvince();
            String carrier = crawlMetadataEntity.getCarrier();
            if (StringUtils.isBlank(crawlChannel)
                    || StringUtils.isBlank(province)
                    || StringUtils.isBlank(carrier)) {
                log.error("carrier:[{}],province:[{}],crawlChannel:[{}],某一必须参数信息为空", carrier, province, crawlChannel);
                return;
            }
            ParserMonitorMessage monitorMessage = newParserMonitorMessage(crawlMetadataEntity, crawlChannel);
            //按运营商-省份-通道分组统计：CHINA_MOBILE-GANSU-APP_API
            String keyPre = String.format("%s-%s-%s", carrier, province, crawlChannel);
            //需要校验的字段
            CarrierVerifty carrierVerify = carrierVerifyService.getCarrierVerify(carrier, province, crawlChannel);
            List<CrawlerMetadataModel> metadataModelList = new ArrayList<>();
            addMetadaModelList(crawlMetadataEntity, metadataModelList);

            if (parserMonitorInfo.getMobileDetailInfo() != null) {
                //校验
                metadataModelList.stream().forEach(v -> componentMap.get(v.getType()).verifyData(v, parserMonitorInfo.getMobileDetailInfo(), carrierVerify, keyPre,monitorMessage));
            }
        }
    }

    private void addMetadaModelList(CrawlMetadataEntity crawlMetadataEntity, List<CrawlerMetadataModel> metadataModelList) {
        if (getDetailMetadataModel(crawlMetadataEntity.getBill()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getBill()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getCall()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getCall()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getSms()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getSms()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getNet()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getNet()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getPackageUsage()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getPackageUsage()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getBasic()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getBasic()));
        }
        if (getDetailMetadataModel(crawlMetadataEntity.getRecharge()) != null) {
            metadataModelList.add(getDetailMetadataModel(crawlMetadataEntity.getRecharge()));
        }
    }

    private CrawlerMetadataModel getDetailMetadataModel(String detailMetadata) {
        return BaseJsonUtils.readValue(detailMetadata, CrawlerMetadataModel.class);
    }


    /**
     * 构建某一任务的基本信息
     * */
    protected ParserMonitorMessage newParserMonitorMessage(CrawlMetadataEntity crawlMetadataEntity,String channel){
        ParserMonitorMessage monitorMessage = new ParserMonitorMessage();
        monitorMessage.setTenantId(crawlMetadataEntity.getTenantId());
        monitorMessage.setTaskId(crawlMetadataEntity.getTaskId());
        monitorMessage.setProvince(crawlMetadataEntity.getProvince());
        monitorMessage.setMobile(crawlMetadataEntity.getMobile());
        monitorMessage.setChannel(channel);
        monitorMessage.setCarrier(crawlMetadataEntity.getCarrier());
        return monitorMessage;
    }
}
