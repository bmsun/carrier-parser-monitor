package com.moxie.cloud.services.server;

import com.moxie.cloud.service.server.service.MoxieMicroService;
import com.moxie.cloud.service.server.service.MoxieServiceDocable;
import com.moxie.cloud.service.server.service.MoxieServiceInfo;
import com.moxie.cloud.services.common.MonitorServiceConstants;
import com.moxie.cloud.services.server.kafka.KafkaHandler;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.service.ApiInfo;

@SpringBootApplication
@ComponentScan(basePackages = {"com.moxie.cloud.services"})
public class CarrierParserMonitorMicroService implements MoxieServiceDocable {

	@Override
	@Bean
	public MoxieServiceInfo getMoxieServiceInfo() {
		ApiInfo apiInfo =  new ApiInfo("parser monitor Api", "parser-monitor相关api.", MonitorServiceConstants.VERSION, null, ApiInfo.DEFAULT_CONTACT, null, null);
		MoxieServiceInfo serviceInfo = new MoxieServiceInfo(apiInfo, MonitorServiceConstants.SERVER_NAME);
		return serviceInfo;
	}

	public static void main(String[] args) {
		MoxieMicroService app = new MoxieMicroService();
		ApplicationContext applicationContext = app.start(args);

		KafkaHandler kafkaHandler = (KafkaHandler) applicationContext.getBean("kafkaHandler");
		kafkaHandler.startConsuming();
	}
}
