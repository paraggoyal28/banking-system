package com.banking.paymentservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;

@Configuration
public class coreConfig {

	@Bean
	public WebMvcConfigurer coreConfigurer() {
		return new WebMvcConfigurer() {
			public void addCoreMappings(CorsRegistry registry) {
				registry.addMapping("/api/**")
						.allowedHeaders("*")
						.allowedMethods("GET", "POST", "PUT", "DELETE");
			}
		};
	}
}
