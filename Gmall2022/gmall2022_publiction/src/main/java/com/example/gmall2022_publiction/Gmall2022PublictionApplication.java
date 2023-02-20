package com.example.gmall2022_publiction;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.example.gmall2022_publiction.mapper")
public class Gmall2022PublictionApplication {

	public static void main(String[] args) {
		SpringApplication.run(Gmall2022PublictionApplication.class, args);
	}

}
