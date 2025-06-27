package com.minimax.comparator.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态数据源配置类
 * 支持运行时动态创建和管理多个数据源
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@Configuration
public class DynamicDataSourceConfig {

    @Autowired
    private ComparisonProperties comparisonProperties;

    /**
     * 数据源缓存
     */
    private final Map<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

    /**
     * JdbcTemplate缓存
     */
    private final Map<String, JdbcTemplate> jdbcTemplateCache = new ConcurrentHashMap<>();

    /**
     * 获取数据源
     * 
     * @param dataSourceName 数据源名称
     * @return DataSource
     */
    public DataSource getDataSource(String dataSourceName) {
        return dataSourceCache.computeIfAbsent(dataSourceName, this::createDataSource);
    }

    /**
     * 获取JdbcTemplate
     * 
     * @param dataSourceName 数据源名称
     * @return JdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate(String dataSourceName) {
        return jdbcTemplateCache.computeIfAbsent(dataSourceName, 
            dsName -> new JdbcTemplate(getDataSource(dsName)));
    }

    /**
     * 创建数据源
     * 
     * @param dataSourceName 数据源名称
     * @return DataSource
     */
    private DataSource createDataSource(String dataSourceName) {
        ComparisonProperties.DataSourceConfig config = findDataSourceConfig(dataSourceName);
        if (config == null) {
            throw new IllegalArgumentException("未找到数据源配置: " + dataSourceName);
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setDriverClassName(config.getDriverClassName());

        // 设置连接池参数
        hikariConfig.setMaximumPoolSize(20);
        hikariConfig.setMinimumIdle(5);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);

        // 设置连接池名称
        hikariConfig.setPoolName("HikariCP-" + dataSourceName);

        // 应用自定义属性
        if (config.getProperties() != null) {
            config.getProperties().forEach(hikariConfig::addDataSourceProperty);
        }

        return new HikariDataSource(hikariConfig);
    }

    /**
     * 查找数据源配置
     * 
     * @param dataSourceName 数据源名称
     * @return DataSourceConfig
     */
    private ComparisonProperties.DataSourceConfig findDataSourceConfig(String dataSourceName) {
        if (comparisonProperties.getDataSources() == null) {
            return null;
        }
        return comparisonProperties.getDataSources().stream()
                .filter(config -> dataSourceName.equals(config.getName()))
                .findFirst()
                .orElse(null);
    }

    /**
     * 获取所有数据源名称
     * 
     * @return Set<String>
     */
    public java.util.Set<String> getAllDataSourceNames() {
        if (comparisonProperties.getDataSources() == null) {
            return java.util.Collections.emptySet();
        }
        return comparisonProperties.getDataSources().stream()
                .map(ComparisonProperties.DataSourceConfig::getName)
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * 测试数据源连接
     * 
     * @param dataSourceName 数据源名称
     * @return boolean
     */
    public boolean testConnection(String dataSourceName) {
        try {
            JdbcTemplate jdbcTemplate = getJdbcTemplate(dataSourceName);
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 关闭所有数据源
     */
    public void closeAllDataSources() {
        dataSourceCache.values().forEach(dataSource -> {
            if (dataSource instanceof HikariDataSource) {
                ((HikariDataSource) dataSource).close();
            }
        });
        dataSourceCache.clear();
        jdbcTemplateCache.clear();
    }
}
