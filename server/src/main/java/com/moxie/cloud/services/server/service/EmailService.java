/**
 * Create By Hangzhou Moxie Data Technology Co. Ltd.
 */
package com.moxie.cloud.services.server.service;

import com.moxie.cloud.services.server.config.MailConfig;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.stereotype.Service;
import org.springframework.ui.velocity.VelocityEngineUtils;

import java.util.Map;

/**
 * 邮件发送服务实现
 *
 * @author zhangpengfei
 * @version Id: EmailServiceImpl.java,V 1.0 2016/05/13 上午1:26 zhangpengfei Exp $
 */
@Service
public class EmailService {

    private static final Logger log = LoggerFactory.getLogger(EmailService.class);
    private static final String CHARSET_UTF8 = "UTF-8";

    @Autowired
    private JavaMailSender javaMailSender;

    @Autowired
    private VelocityEngine velocityEngine;

    @Autowired
    private MailConfig mailConfig;

    public void sendEmail(final Map<String, Object> model, final String subject, final String vmfile) {
        String[] to = mailConfig.getTo().split(",");    // 默认接收人地址列表
        sendEmail(model, subject, to, vmfile);
    }

    public void sendEmail(Map<String, Object> model, String subject, String[] to, String vmfile) {

        MimeMessagePreparator preparator = mimeMessage -> {
            MimeMessageHelper message = new MimeMessageHelper(mimeMessage, true, "GBK");
            message.setTo(to);//设置接收方的email地址
            message.setSubject(subject);//设置邮件主题
            message.setFrom(mailConfig.getFrom());//设置发送方地址
            String text = VelocityEngineUtils.mergeTemplateIntoString(
                    velocityEngine, vmfile, CHARSET_UTF8, model);
            message.setText(text, true);
        };
        int times = 3;
        for (int i = 0; i < times; i++) {
            try {
                this.javaMailSender.send(preparator);//发送邮件
                break;
            } catch (Exception e) {
                // e.printStackTrace();
                continue;
            }
        }
    }
}
