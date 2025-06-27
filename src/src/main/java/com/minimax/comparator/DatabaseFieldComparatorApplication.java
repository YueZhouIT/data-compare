package com.minimax.comparator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * 数据库字段比较器主应用
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
public class DatabaseFieldComparatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatabaseFieldComparatorApplication.class, args);
    }
}
