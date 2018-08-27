package com.moxie.cloud.services.server.service;

import com.moxie.cloud.services.common.constants.MonitorModule;
import com.moxie.cloud.services.common.utils.MailToUtils;
import com.moxie.cloud.services.msgSend.client.MsgSendServiceClient;
import com.moxie.cloud.services.msgSend.common.dto.MarkDownParamEntity;
import com.moxie.commons.MoxieDateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@Service
@Slf4j
public class MonitorService {
    @Value("${mail.to.leader}")
    protected String leader;

    @Value("${mail.to.monitor}")
    protected String monitor;

    @Value("${mail.to.leader.module}")
    protected String modules;

    private static final String FLAG=" \\x0d+ ";

    @Autowired
    MsgSendServiceClient msgSendServiceClient;

    @Autowired
    private EmailService emailService;


    public void sendMonitorMail(Object info,long count,long timestamp) {
        log.info("send:{}",info.toString());
        sendMailTo((String)info,count,timestamp);
    }

    private void sendMailTo(String subject,long count,long timestamp){
        Date endTime = new Date();
        DateTime startTime = new DateTime(timestamp);
        String[] to = MailToUtils.mailTo(monitor, leader, modules, MonitorModule.QUERY);
        //String[] to = {"luwei@51dojo.com"};
        log.info("{}-{}-{},使用模版{},接收人{},", MoxieDateUtils.toChinaShortStr(startTime.toDate()),
                MoxieDateUtils.toChinaShortStr(endTime), subject, "statistic-quality.vm", to) ;
        Map<String,Object> maps = new HashMap<>();
        maps.put("subject",subject);
        maps.put("startTime",startTime);
        maps.put("finishTime",endTime);
        //maps.put("info",info);
        maps.put("count",count);
        emailService.sendEmail(maps,subject,to,"statistic-quality.vm");
    }

    public void sendDingRobot(String[] webhookTokens, String errorMsg, BigDecimal useTime, Long count, int maxErrorCount) {
        if (StringUtils.isBlank(errorMsg)) return;
        MarkDownParamEntity markDownParamEntity = new MarkDownParamEntity();
        markDownParamEntity.setBusinessType("carrier");
        markDownParamEntity.setMonitorPoint("parser-monitor");
        markDownParamEntity.setSendTime(new DateTime().toString("yyyy-MM-dd HH:mm:ss"));
        markDownParamEntity.setTitle("实时监控告警");
        markDownParamEntity.setEmergencyLevel("重要");
        markDownParamEntity.setWebHookToken(webhookTokens);
        markDownParamEntity.setText(buildText(errorMsg,useTime,count,maxErrorCount));
        msgSendServiceClient.DingRobotMarkDown(markDownParamEntity);
    }

    private String buildText(String errorMsg, BigDecimal useTime, Long count,int maxErrorCount) {
        String text="**省份通道:"+errorMsg.split(":")[0]+"**"+ FLAG+"单位时间:"+useTime+"分钟内"+ FLAG+"**告警指标:"+errorMsg.split(":")[1]+"**"+ FLAG+"**异常任务数:"+count+"**"+ FLAG+"告警阀值:"+maxErrorCount;
        return text;
    }
}
