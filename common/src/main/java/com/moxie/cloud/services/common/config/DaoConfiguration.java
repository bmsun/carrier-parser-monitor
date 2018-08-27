package com.moxie.cloud.services.common.config;


import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author zhanghesheng
 * @Description jdbc连接配置类.
 */

@Configuration  //配置类注解
@EnableTransactionManagement //spring事务
@ConditionalOnProperty("datasource.url")
public  class DaoConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(DaoConfiguration.class);
    private Map<String, Object> datasourceMap;

    @Autowired
    //读取配置文件application.properties的类
    private Environment env;

    @Value("${datasource.url:jdbc:mysql://192.168.0.10:3306/taskarchive?characterEncoding=utf8&amp;allowMultiQueries=true}")
    private String url;
    //从application.properties中获取demo.mysql.user的值，若没有，使用默认值test
    @Value("${datasource.user:test2}")
    private String user;
    @Value("${datasource.pass:mx}")
    private String pass;
    @Value("${datasource.initial.size:20}")
    private String initialSize;
    @Value("${datasource.max.active:50}")
    private String maxActive;

    @Value("${datasource.driverClassName}")
    private String driverClassName;

    @PostConstruct
    public void init() {
        datasourceMap = new HashMap<String, Object>();
        datasourceMap.put("driverClassName", driverClassName);
        datasourceMap.put("initialSize", initialSize);
        datasourceMap.put("maxActive", maxActive);
        datasourceMap.put("minIdle", "1");
        datasourceMap.put("maxWait", "20000");
        datasourceMap.put("removeAbandoned", "true");
        datasourceMap.put("removeAbandonedTimeout", "180");
        datasourceMap.put("timeBetweenEvictionRunsMillis", "60000");
        datasourceMap.put("minEvictableIdleTimeMillis", "300000");
        datasourceMap.put("validationQuery", "SELECT 1");
        datasourceMap.put("testWhileIdle", "true");
        datasourceMap.put("testOnBorrow", "false");
        datasourceMap.put("testOnReturn", "false");
        datasourceMap.put("poolPreparedStatements", "true");
        datasourceMap.put("maxPoolPreparedStatementPerConnectionSize", "50");
        datasourceMap.put("initConnectionSqls", "SELECT 1");

        for (Iterator<PropertySource<?>> it = ((AbstractEnvironment) env).getPropertySources().iterator(); it.hasNext(); ) {
            PropertySource<?> propertySource = it.next();
            this.getPropertiesFromSource(propertySource, datasourceMap);
        }
    }

    @Bean(name = "taskArchiveSource")
     @Qualifier("taskArchiveSource")
    public DataSource taskArchiveSource() {
        LOGGER.info("初始化数据源");
        return this.getDataSource(url, user, pass);
    }

    @Bean(name = "taskArchiveTemplate")
    public JdbcTemplate taskArchiveTemplate() {
        return new JdbcTemplate(this.taskArchiveSource());
    }


    @Bean
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(taskArchiveSource());
    }

    private DataSource getDataSource(String url, String user, String pass) {
        datasourceMap.put(DruidDataSourceFactory.PROP_URL, url);
        datasourceMap.put(DruidDataSourceFactory.PROP_USERNAME, user);
        datasourceMap.put(DruidDataSourceFactory.PROP_PASSWORD, pass);

        try {
            return DruidDataSourceFactory.createDataSource(datasourceMap);
        } catch (Exception e) {
            LOGGER.error("无法获得数据源[{}]:[{}]", url, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException("无法获得数据源.");
        }
    }

    private void getPropertiesFromSource(PropertySource<?> propertySource, Map<String, Object> map) {
        if (propertySource instanceof MapPropertySource) {
            for (String key : ((MapPropertySource) propertySource).getPropertyNames()) {
                map.put(key, propertySource.getProperty(key));
            }
        }
        if (propertySource instanceof CompositePropertySource) {
            for (PropertySource<?> s : ((CompositePropertySource) propertySource).getPropertySources()) {
                getPropertiesFromSource(s, map);
            }
        }
    }
}