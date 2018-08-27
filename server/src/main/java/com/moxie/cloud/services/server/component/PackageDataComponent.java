package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.moxie.cloud.carrier.entity.PackageUsageEntity;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("packageDataComponent")
public class PackageDataComponent extends BaseComponent {


    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage packageMessage = copyParserMonitorMessage(monitorMessage);
        packageMessage.setType(this.getType());
        Map<String, List<String>> packageCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (packageCrawlerMap == null || packageCrawlerMap.isEmpty()) return;
        verifyPackages(packageCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, packageMessage);

    }

    private void verifyPackages(Map<String, List<String>> packageCrawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage packageMessage) {
        List<BillModel> billModelList = mobileDetailInfo.getBillModelList();
        List<PackageUsageEntity> packageUsageEntityList = Lists.newArrayList();
        if (billModelList != null) {
            for (BillModel billModel : billModelList) {
                List<PackageUsageEntity> packageUsageList = billModel.getPackageUsageList();
                if (!CollectionUtils.isEmpty(packageUsageList)) {
                    packageUsageEntityList.addAll(packageUsageList);
                }
            }
        }
        Map<String, Long> packageUsageMap = groupByGetTotal(packageUsageEntityList);
        if (packageUsageMap.size() == 0) {
            //套餐信息未采集到或未解析成功
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_NO_DATA), DataMonitorConstants.PACKAGE_NO_DATA);
            ErrorMessage errorMessage = new ErrorMessage();
            errorMessage.setMessage(DataMonitorConstants.PACKAGE_NO_DATA);
            List<ErrorMessage> errorMessageList = Lists.newArrayList();
            errorMessageList.add(errorMessage);
            buildMessage(packageCrawlerMap.size(), packageMessage, CodeEnum.CODE_NO_DATA, errorMessageList);
            //入库
            addMessageToDb(packageMessage);
            return;
        } else {
            Pair<Integer, Integer> resultPair = checkData(packageCrawlerMap, packageUsageMap);
            int crawlErrorNum = resultPair.getLeft();
            int parserErrorNum = resultPair.getRight();
            if (crawlErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_CRAWL_ERROR), DataMonitorConstants.PACKAGE_CRAWL_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(packageMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.PACKAGE_CRAWL_ERROR);
                List<ErrorMessage> errorMessageList = org.testng.collections.Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(crawlErrorNum, monitorMessage, CodeEnum.CODE_CRAWL_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
            if (parserErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_MONTH_NO_DATA), DataMonitorConstants.PACKAGE_MONTH_NO_DATA);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(packageMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.PACKAGE_MONTH_NO_DATA);
                List<ErrorMessage> errorMessageList = org.testng.collections.Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(parserErrorNum, monitorMessage, CodeEnum.CODE_PARSER_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
        }
        verifyPackageField(carrierVerify, keyPre, packageUsageEntityList, packageMessage);

    }

    private void verifyPackageField(CarrierVerifty carrierVerify, String keyPre, List<PackageUsageEntity> packageUsageEntityList, ParserMonitorMessage packageMessage) {

        int unitNum = 0;
        int usedErrorNum = 0;
        int billDateSameNum = 0;
        int billDateNotFirstNum = 0;
        int billEndDateBeforeStartNum = 0;
        for (PackageUsageEntity packageEntity : packageUsageEntityList) {
            //单位为空
            if (StringUtils.isBlank(packageEntity.getUnit())) {
                unitNum++;
            }
            //已使用的大于总数
            if (Long.valueOf(packageEntity.getUsed()) > Long.valueOf(packageEntity.getTotal())) {
                usedErrorNum++;
            }
            //账单日期
            if (packageEntity.getBillEndDate().before(packageEntity.getBillStartDate())) {
                billEndDateBeforeStartNum++;
            } else if (packageEntity.getBillEndDate().equals(packageEntity.getBillStartDate())) {
                billDateSameNum++;
            } else if (!new DateTime(packageEntity.getBillStartDate()).toString("yyyyMMdd").endsWith("01")) {
                billDateNotFirstNum++;
            }
        }
        List<ErrorMessage> messageList = Lists.newArrayList();

        //单位为空
        if (unitNum > 0) {
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_UNIT_BLANK), DataMonitorConstants.PACKAGE_UNIT_BLANK);
            addErrorMsg("unit", DataMonitorConstants.PACKAGE_UNIT_BLANK, messageList, unitNum, packageUsageEntityList.size());

        }
        //已使用的大于总数
        if (usedErrorNum > 0) {
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_USAGE_INVALID), DataMonitorConstants.PACKAGE_USAGE_INVALID);
            addErrorMsg("total", DataMonitorConstants.PACKAGE_USAGE_INVALID, messageList, usedErrorNum, packageUsageEntityList.size());
        }

        if (billEndDateBeforeStartNum > 0
                || billDateSameNum > 0
                || billDateNotFirstNum > 0) {
            //告警统一
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.PACKAGE_DATE_EXTRACT_ERROR), DataMonitorConstants.PACKAGE_DATE_EXTRACT_ERROR);
            //入库细化
            if (billEndDateBeforeStartNum > 0) {
                addErrorMsg("billDate", DataMonitorConstants.PACKAGE_ENDDATE_BEFORE_START_ERROR, messageList, billEndDateBeforeStartNum, packageUsageEntityList.size());
            }
            if (billDateSameNum > 0) {
                addErrorMsg("billDate", DataMonitorConstants.PACKAGE_ENDDATE_START_SAME, messageList, billDateSameNum, packageUsageEntityList.size());
            }
            if (billDateNotFirstNum > 0) {
                addErrorMsg("billDate", DataMonitorConstants.PACKAGE_DATE_NOT_FIRSTDAY_ERROR, messageList, billDateNotFirstNum, packageUsageEntityList.size());
            }

            //入库
            if (messageList.size() > 0) {
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(packageMessage);
                buildMessage(0, monitorMessage, CodeEnum.CODE_DATA_INVALIDE, messageList);
                addMessageToDb(monitorMessage);
            }
        }
    }


    //按月份分组，并得到每个月总数据数totalNum
    private Map<String, Long> groupByGetTotal(List<PackageUsageEntity> packageUsageEntities) {
        Map<String, Long> results = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(packageUsageEntities)) {
            results = packageUsageEntities
                    .parallelStream()
                    .collect(Collectors.groupingBy(packageUsage -> formatMonth(packageUsage.getBillStartDate()), Collectors.counting()));
        }
        return results;
    }

    @Override
    public String getType() {
        return DataTypeEnum.PACKAGE_USAGE.name();
    }
}
