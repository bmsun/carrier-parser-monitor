/**
 * Create By Hangzhou Moxie Data Technology Co. Ltd.
 */
package com.moxie.cloud.services.server.config;

import com.moxie.commons.MoxieBeanUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;

/**
 * 邮件配置
 *
 * @author zhangpengfei
 * @version Id: MailConfig.java,V 1.0 2016/05/12 下午11:52 zhangpengfei Exp $
 */
@Configuration
@ConfigurationProperties(prefix = "mail")
public class MailConfig {

    public static class Smtp {

        private boolean auth;
        private boolean starttlsEnable;
        private boolean required;

        public boolean isAuth() {
            return auth;
        }

        public boolean isStarttlsEnable() {
            return starttlsEnable;
        }

        public void setAuth(boolean auth) {
            this.auth = auth;
        }

        public void setStarttlsEnable(boolean starttlsEnable) {
            this.starttlsEnable = starttlsEnable;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }


        @Override
        public String toString() {
            return MoxieBeanUtils.getJsonString(this);
        }
    }

    @NotBlank
    private String host;
    private int port;
    private String from;
    private String username;
    private String password;
    private String protocol;
    private String to;
    @NotNull
    private Smtp smtp;

    public String getProtocol() {
        return protocol;
    }


    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Smtp getSmtp() {
        return smtp;
    }

    public void setSmtp(Smtp smtp) {
        this.smtp = smtp;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
