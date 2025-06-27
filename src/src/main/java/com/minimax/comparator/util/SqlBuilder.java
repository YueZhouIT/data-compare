package com.minimax.comparator.util;

import com.minimax.comparator.config.ComparisonProperties;
import org.apache.commons.lang3.StringUtils;

/**
 * SQL构建工具类
 * 提供灵活的SQL构建功能
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
public class SqlBuilder {

    /**
     * 构建基础查询SQL
     * 
     * @param tableConfig 表配置
     * @param selectFields 选择字段
     * @param whereCondition WHERE条件
     * @return SQL语句
     */
    public static String buildSelectSql(ComparisonProperties.TableConfig tableConfig, 
                                       String selectFields, 
                                       String whereCondition) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(selectFields);
        sql.append(" FROM ").append(tableConfig.getFullTableName());
        
        if (StringUtils.isNotBlank(whereCondition)) {
            sql.append(" WHERE ").append(whereCondition);
        }
        
        return sql.toString();
    }

    /**
     * 构建分页查询SQL
     * 
     * @param tableConfig 表配置
     * @param selectFields 选择字段
     * @param whereCondition WHERE条件
     * @param orderBy 排序字段
     * @param offset 偏移量
     * @param limit 限制数量
     * @param databaseType 数据库类型
     * @return SQL语句
     */
    public static String buildPagedSelectSql(ComparisonProperties.TableConfig tableConfig,
                                           String selectFields,
                                           String whereCondition,
                                           String orderBy,
                                           int offset,
                                           int limit,
                                           DatabaseType databaseType) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(selectFields);
        sql.append(" FROM ").append(tableConfig.getFullTableName());
        
        if (StringUtils.isNotBlank(whereCondition)) {
            sql.append(" WHERE ").append(whereCondition);
        }
        
        if (StringUtils.isNotBlank(orderBy)) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        
        // 根据数据库类型添加分页语法
        switch (databaseType) {
            case MYSQL:
            case POSTGRESQL:
                sql.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
                break;
            case ORACLE:
                // Oracle 12c+ 语法
                sql.append(" OFFSET ").append(offset).append(" ROWS FETCH NEXT ").append(limit).append(" ROWS ONLY");
                break;
            case SQL_SERVER:
                sql.append(" OFFSET ").append(offset).append(" ROWS FETCH NEXT ").append(limit).append(" ROWS ONLY");
                break;
            default:
                // 默认使用LIMIT语法
                sql.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        }
        
        return sql.toString();
    }

    /**
     * 构建计数SQL
     * 
     * @param tableConfig 表配置
     * @param whereCondition WHERE条件
     * @return SQL语句
     */
    public static String buildCountSql(ComparisonProperties.TableConfig tableConfig, String whereCondition) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(*)");
        sql.append(" FROM ").append(tableConfig.getFullTableName());
        
        if (StringUtils.isNotBlank(whereCondition)) {
            sql.append(" WHERE ").append(whereCondition);
        }
        
        return sql.toString();
    }

    /**
     * 构建字段存在性检查SQL
     * 
     * @param tableConfig 表配置
     * @param fieldName 字段名
     * @param databaseType 数据库类型
     * @return SQL语句
     */
    public static String buildFieldExistsSql(ComparisonProperties.TableConfig tableConfig, 
                                           String fieldName, 
                                           DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
                return String.format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                        tableConfig.getSchema() != null ? tableConfig.getSchema() : "DATABASE()",
                        tableConfig.getTableName(), fieldName);
            case POSTGRESQL:
                return String.format("SELECT column_name FROM information_schema.columns " +
                        "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                        tableConfig.getSchema() != null ? tableConfig.getSchema() : "public",
                        tableConfig.getTableName(), fieldName);
            case ORACLE:
                return String.format("SELECT COLUMN_NAME FROM USER_TAB_COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                        tableConfig.getTableName().toUpperCase(), fieldName.toUpperCase());
            default:
                return String.format("SELECT '%s' AS column_name", fieldName);
        }
    }

    /**
     * 构建表存在性检查SQL
     * 
     * @param tableConfig 表配置
     * @param databaseType 数据库类型
     * @return SQL语句
     */
    public static String buildTableExistsSql(ComparisonProperties.TableConfig tableConfig, 
                                           DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
                return String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        tableConfig.getSchema() != null ? tableConfig.getSchema() : "DATABASE()",
                        tableConfig.getTableName());
            case POSTGRESQL:
                return String.format("SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = '%s' AND table_name = '%s'",
                        tableConfig.getSchema() != null ? tableConfig.getSchema() : "public",
                        tableConfig.getTableName());
            case ORACLE:
                return String.format("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME = '%s'",
                        tableConfig.getTableName().toUpperCase());
            default:
                return String.format("SELECT '%s' AS table_name", tableConfig.getTableName());
        }
    }

    /**
     * 根据JDBC URL推断数据库类型
     * 
     * @param jdbcUrl JDBC URL
     * @return DatabaseType
     */
    public static DatabaseType inferDatabaseType(String jdbcUrl) {
        if (jdbcUrl == null) {
            return DatabaseType.UNKNOWN;
        }
        
        String url = jdbcUrl.toLowerCase();
        if (url.contains("mysql")) {
            return DatabaseType.MYSQL;
        } else if (url.contains("postgresql")) {
            return DatabaseType.POSTGRESQL;
        } else if (url.contains("oracle")) {
            return DatabaseType.ORACLE;
        } else if (url.contains("sqlserver")) {
            return DatabaseType.SQL_SERVER;
        } else if (url.contains("h2")) {
            return DatabaseType.H2;
        } else {
            return DatabaseType.UNKNOWN;
        }
    }

    /**
     * 数据库类型枚举
     */
    public enum DatabaseType {
        MYSQL("MySQL"),
        POSTGRESQL("PostgreSQL"),
        ORACLE("Oracle"),
        SQL_SERVER("SQL Server"),
        H2("H2"),
        UNKNOWN("Unknown");
        
        private final String displayName;
        
        DatabaseType(String displayName) {
            this.displayName = displayName;
        }
        
        public String getDisplayName() {
            return displayName;
        }
    }

    /**
     * 转义SQL标识符
     * 
     * @param identifier 标识符
     * @param databaseType 数据库类型
     * @return 转义后的标识符
     */
    public static String escapeIdentifier(String identifier, DatabaseType databaseType) {
        if (identifier == null) {
            return null;
        }
        
        switch (databaseType) {
            case MYSQL:
                return "`" + identifier.replace("`", "``") + "`";
            case POSTGRESQL:
            case H2:
                return "\"" + identifier.replace("\"", "\"\"") + "\"";
            case ORACLE:
            case SQL_SERVER:
                return "[" + identifier.replace("]", "]]") + "]";
            default:
                return identifier;
        }
    }

    /**
     * 构建安全的IN条件SQL
     * 
     * @param fieldName 字段名
     * @param valueCount 值的数量
     * @return SQL条件
     */
    public static String buildInCondition(String fieldName, int valueCount) {
        if (valueCount <= 0) {
            return "1=0"; // 返回永远为false的条件
        }
        
        StringBuilder condition = new StringBuilder();
        condition.append(fieldName).append(" IN (");
        for (int i = 0; i < valueCount; i++) {
            if (i > 0) {
                condition.append(", ");
            }
            condition.append("?");
        }
        condition.append(")");
        
        return condition.toString();
    }
}
