package com.moxie.cloud.services.server.config;

import com.moxie.cloud.carrier.CarrierDaoConfiguration;
import com.moxie.cloud.service.server.config.BaseConfig;
import com.moxie.cloud.services.msgSend.client.MsgSendServiceClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;


@Configuration
@Import({CarrierDaoConfiguration.class})
public class AppConfiguration extends BaseConfig {
   @Autowired
   AppProperties appProperties;
   @Autowired
   MailConfig mailConfig;

   @Bean(name = "msgSendServiceClient")
   public MsgSendServiceClient msgSendServiceClient(){
      return new MsgSendServiceClient(appProperties.getMsgSendTag());
   }


   @Bean
   public JavaMailSender javaMailSender() {
      JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
      mailSender.setJavaMailProperties(getMailProperties());
      mailSender.setHost(mailConfig.getHost());
      mailSender.setPort(mailConfig.getPort());
      mailSender.setProtocol(mailConfig.getProtocol());
      mailSender.setUsername(mailConfig.getUsername());
      mailSender.setPassword(mailConfig.getPassword());
      return mailSender;
   }

   private Properties getMailProperties() {
      Properties properties = new Properties();
      properties.setProperty("mail.smtp.auth", "true");
      properties.setProperty("mail.smtp.starttls.enable", String.valueOf(mailConfig.getSmtp().isStarttlsEnable()));
      properties.setProperty("mail.debug", "false");
      return properties;
   }
   }
