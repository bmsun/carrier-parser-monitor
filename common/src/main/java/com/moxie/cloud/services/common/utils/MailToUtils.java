package com.moxie.cloud.services.common.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * Created by yajun on 8/31/16.
 * 接收邮件人
 */
public class MailToUtils {

    /**
     * 组装发送邮件的收件人
     * @param monitor 监控开发的邮箱
     * @param leader 发送领导的邮箱
     * @param modules 老板关注的模块
     * @param module 监控的模块
     * @return
     */
    public static String[] mailTo(String[] monitor, String[] leader, String modules, String module) {
        if(StringUtils.isNotBlank(modules) && modules.contains(module)) {
            return ArrayUtils.addAll(monitor, leader);
        }
        return monitor;
    }

    public static String[] mailTo(String monitor, String leader, String modules, String module) {
        String[] monitors = monitor.split(",");
        String[] leaders = leader.split(",");
        return mailTo(monitors, leaders, modules, module);
    }
    public static String[] excludeMail(String[] mailTo,String excludes){
        return Arrays.stream(mailTo).filter(v->!v.equals(excludes)).toArray(String[]::new);
    }

 /*   public static void main(String[] args) {
        String mo = "18116273323@189.cn,825078383@qq.com";
        String le = "zhangpengfei@51dojo.com,dongyajun@51dojo.com";
        String[] result = mailTo(mo.split(","), le.split(","), "tenant,", MonitorModule.QUERY);
        String[] re = excludeMail(result,"18116273323@189.cn");
        for (int i = 0;i < re.length ; i++) {
            System.out.print(re[i]);
        }
    }*/
}
