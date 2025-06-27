package com.minimax.comparator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 比较配置属性类
 * 通过此类可以灵活配置不同的比较规则
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@Component
@ConfigurationProperties(prefix = "comparator")
public class ComparisonProperties {
    
    /**
     * 数据源配置列表
     */
    private List<DataSourceConfig> dataSources;
    
    /**
     * 比较规则配置列表
     */
    private List<ComparisonRule> rules;
    
    /**
     * 批处理大小，用于优化性能
     */
    private int batchSize = 1000;
    
    /**
     * 是否启用并行处理
     */
    private boolean enableParallel = true;
    
    /**
     * 线程池大小
     */
    private int threadPoolSize = 10;

    // Getters and Setters
    public List<DataSourceConfig> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<DataSourceConfig> dataSources) {
        this.dataSources = dataSources;
    }

    public List<ComparisonRule> getRules() {
        return rules;
    }

    public void setRules(List<ComparisonRule> rules) {
        this.rules = rules;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isEnableParallel() {
        return enableParallel;
    }

    public void setEnableParallel(boolean enableParallel) {
        this.enableParallel = enableParallel;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    /**
     * 数据源配置
     */
    public static class DataSourceConfig {
        private String name;
        private String url;
        private String username;
        private String password;
        private String driverClassName;
        private Map<String, String> properties;

        // Getters and Setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
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

        public String getDriverClassName() {
            return driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    /**
     * 比较规则配置
     */
    public static class ComparisonRule {
        private String name;
        private String description;
        private TableConfig sourceTable;
        private TableConfig targetTable;
        private String compareField;
        private String keyField;
        private String whereCondition;
        private boolean enabled = true;

        // Getters and Setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public TableConfig getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(TableConfig sourceTable) {
            this.sourceTable = sourceTable;
        }

        public TableConfig getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(TableConfig targetTable) {
            this.targetTable = targetTable;
        }

        public String getCompareField() {
            return compareField;
        }

        public void setCompareField(String compareField) {
            this.compareField = compareField;
        }

        public String getKeyField() {
            return keyField;
        }

        public void setKeyField(String keyField) {
            this.keyField = keyField;
        }

        public String getWhereCondition() {
            return whereCondition;
        }

        public void setWhereCondition(String whereCondition) {
            this.whereCondition = whereCondition;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * 表配置
     */
    public static class TableConfig {
        private String dataSource;
        private String tableName;
        private String schema;

        // Getters and Setters
        public String getDataSource() {
            return dataSource;
        }

        public void setDataSource(String dataSource) {
            this.dataSource = dataSource;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getFullTableName() {
            if (schema != null && !schema.trim().isEmpty()) {
                return schema + "." + tableName;
            }
            return tableName;
        }
    }
}
