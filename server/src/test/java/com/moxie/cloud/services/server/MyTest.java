package com.moxie.cloud.services.server;

import com.alibaba.druid.wall.violation.ErrorCode;
import com.moxie.cloud.carrier.entity.BillEntity;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.BillModel;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import com.moxie.cloud.services.common.metadata.MetadateModel;
import com.moxie.cloud.services.common.utils.BaseJsonUtils;
import com.moxie.cloud.services.server.component.BaseComponent;
import com.moxie.cloud.services.server.component.BillDataComponent;
import com.moxie.commons.MoxieBeanUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.testng.collections.Lists;
import org.testng.collections.Maps;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_HALF_UP;

public class MyTest {
    private static final Map<String, BaseComponent> componentMap =new HashMap<>();

    @Before
    public void init(){
        componentMap.put(DataTypeEnum.BILL.name(), new BillDataComponent());
    }

    @Test
    public void testJson(){
        CrawlerMetadataModel crawlerMetadataModel = BaseJsonUtils.readValue("{\"type\":\"BILL\",\"crawler\":[{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201708_20170918165341579.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201708\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201706_20170918165343090.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201706\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201707_20170918165341944.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201707\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201705_20170918165344741.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201705\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201704_20170918165345969.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201704\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201709_20170918165341097.json\",\"errorCode\":\"0\",\"errorMsg\":\"数据采集正常\"}],\"bill_date\":\"201709\"}],\"extractor\":[{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201708_20170918165341579.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201708\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201706_20170918165343090.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201706\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201707_20170918165341944.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201707\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201705_20170918165344741.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201705\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201704_20170918165345969.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201704\"},{\"status\":0,\"errors\":[{\"pageId\":\"history-bill\",\"fileName\":\"history-bill_201709_20170918165341097.json\",\"errorCode\":\"0\",\"errorMsg\":\"解析正常\"}],\"bill_date\":\"201709\"}],\"store\":[{\"status\":0,\"errors\":[{\"errorCode\":\"0\",\"errorMsg\":\"存储正常\"}]}],\"type_name\":\"账单信息\"}", CrawlerMetadataModel.class);
        List<String> list = new ArrayList<MetadateModel>().stream().filter(v-> StringUtils.isBlank(v.getBillDate())).map(v -> v.getBillDate()).collect(Collectors.toList());
        System.out.println(list.size());
       list.stream().forEach(System.out::println);
    }

    @Test
    public void testJsonNull(){
        CrawlerMetadataModel crawlerMetadataModel = BaseJsonUtils.readValue("", CrawlerMetadataModel.class);
        List<String> list = new ArrayList<MetadateModel>().stream().filter(v-> StringUtils.isBlank(v.getBillDate())).map(v -> v.getBillDate()).collect(Collectors.toList());
        System.out.println(list.size());
        list.stream().forEach(System.out::println);
    }


    @Test
    public void testFinal(){
       // System.out.println("46bc9c00-a1f7-11e8-8263-00163e004a23".length());
       // System.out.println(componentMap.get(DataTypeEnum.BILL.name()));
       String str=String.format("%s:%s","CHINA_MOBILE-GANSU-APP_API", DataMonitorConstants.MOBILE_BLANK);
        System.out.println(str);
        System.out.println(str.split(":")[0]+"\t"+str.split(":")[1]);
    }

    @Test
    public void testMap(){
        Map<String, Long> results = Maps.newHashMap();
        System.out.println(results.get("123"));
        System.out.println(results.size());
    }

    @Test
    public void test1(){
        String[] split = "".split(",");
        System.out.println(split.length);
        Arrays.stream(split).forEach(System.out::println);
    }

    @Test
    public void test2(){
        int expireTime=180;
        int ttl=120;
        BigDecimal value = BigDecimal.valueOf(expireTime-ttl).divide(new BigDecimal(60),2,ROUND_HALF_UP);

        System.out.println(value);
        String yyyyMMdd = new DateTime(2018,8,1,0,0).toString("yyyyMMdd");
        System.out.println(yyyyMMdd);
        System.out.println(yyyyMMdd.endsWith("01"));

    }

    @Test
    public void test3(){
        Date yyyyMMdd = new DateTime(2018,8,1,0,0).toDate();
        Date yyyyMMdd1 = new DateTime(2018,8,1,0,0).toDate();

        System.out.println(yyyyMMdd.equals(yyyyMMdd1));
    }

    @Test
    public void testList2Json(){
        List<ErrorMessage> messages = Lists.newArrayList();
        ErrorMessage message =new ErrorMessage();
        message.setMessage(DataMonitorConstants.CALL_NO_DATA);
        messages.add(message);
        System.out.println(MoxieBeanUtils.getJsonString(message));
        System.out.println(MoxieBeanUtils.getJsonString(messages));

    }

    @Test
    public void testListLam(){
        List<BillModel> billModelList =new ArrayList<>();
        //BillModel billModel = new BillModel();
        BillEntity billEntity = new BillEntity();
        //billEntity.setBillStartDate(new Date());
        //billModel.setBillEntity(billEntity);
        //billModelList.add(billModel);
        Map<String, BillEntity> collect = billModelList.stream().map(BillModel::getBillEntity).filter(v -> v != null&&v.getBillStartDate()!=null).collect(Collectors.toMap(v -> formatMonth(v.getBillStartDate()), Function.identity(),(v,id)->v));

        //Collectors.toMap(v->formatMonth(v.getBillStartDate()), Function.identity())
        Set<Map.Entry<String, BillEntity>> strings = collect.entrySet();
        for (Map.Entry<String, BillEntity> entry:strings) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        System.out.println(collect.size());
        System.out.println(collect.get("123"));
    }



    @Test
    public void testList() throws Exception{
        LinkedBlockingQueue<String> queue =new LinkedBlockingQueue<>();
        List<String>list =new ArrayList<>();
        //list.add(null);
        //list.add(null);
        //list.add(null);
        queue.addAll(list);
        System.out.println(queue.size());
        ParserMonitorMessage monitorMessage = buildMessage();

        for (int i=0;i<5;i++) {
            if(i==0){
               // ParserMonitorMessage monitorMessage1 = (ParserMonitorMessage) BeanUtils.cloneBean(monitorMessage);
                //monitorMessage1.setChannel("APP_API");
                //System.out.println(monitorMessage1.getChannel()+"\t"+monitorMessage1.getCarrier());
            } else if(i==1){
                ParserMonitorMessage monitorMessage1 = new ParserMonitorMessage();
                BeanUtils.copyProperties(monitorMessage1,monitorMessage);
                monitorMessage1.setChannel("web_shop");
                System.out.println(monitorMessage1.getChannel()+"\t"+monitorMessage1.getCarrier());
            }else {
                //ParserMonitorMessage monitorMessage1 = (ParserMonitorMessage) BeanUtils.cloneBean(monitorMessage);
                //System.out.println(monitorMessage1.getChannel() + "\t" + monitorMessage1.getCarrier());
            }
        }
    }


    ParserMonitorMessage buildMessage(){
        ParserMonitorMessage monitorMessage =new ParserMonitorMessage();
        monitorMessage.setCarrier("HENAN");
        return monitorMessage;
    }

    //格式化 yyyyMM
    protected String formatMonth(Date date) {
        return date == null ? "" : DateFormatUtils.format(date, "yyyyMM");
    }
}
