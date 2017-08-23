package com.izuanqian.ilivespider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class IliveSpiderApplication {

	public static void main(String[] args) {
		SpringApplication.run(IliveSpiderApplication.class, args);
	}
}
