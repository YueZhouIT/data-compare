package com.minimax.comparator.service;

import com.minimax.comparator.config.ComparisonProperties;
import com.minimax.comparator.config.DynamicDataSourceConfig;
import com.minimax.comparator.model.ComparisonResult;
import com.minimax.comparator.model.DifferenceDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 字段比较服务类
 * 提供高效的数据库字段比较功能
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@Service
public class FieldComparisonService {

    private static final Logger logger = LoggerFactory.getLogger(FieldComparisonService.class);

    @Autowired
    private ComparisonProperties comparisonProperties;

    @Autowired
    private DynamicDataSourceConfig dataSourceConfig;

    /**
     * 执行所有启用的比较规则
     * 
     * @return List<ComparisonResult>
     */
    public List<ComparisonResult> executeAllComparisons() {
        List<ComparisonProperties.ComparisonRule> enabledRules = getEnabledRules();
        
        if (comparisonProperties.isEnableParallel()) {
            return executeComparisonsInParallel(enabledRules);
        } else {
            return executeComparisonsSequentially(enabledRules);
        }
    }

    /**
     * 执行单个比较规则
     * 
     * @param ruleName 规则名称
     * @return ComparisonResult
     */
    public ComparisonResult executeComparison(String ruleName) {
        ComparisonProperties.ComparisonRule rule = findRuleByName(ruleName);
        if (rule == null) {
            throw new IllegalArgumentException("未找到规则: " + ruleName);
        }
        
        return performComparison(rule);
    }

    /**
     * 执行指定的比较规则列表
     * 
     * @param ruleNames 规则名称列表
     * @return List<ComparisonResult>
     */
    public List<ComparisonResult> executeComparisons(List<String> ruleNames) {
        List<ComparisonProperties.ComparisonRule> rules = ruleNames.stream()
                .map(this::findRuleByName)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        if (comparisonProperties.isEnableParallel()) {
            return executeComparisonsInParallel(rules);
        } else {
            return executeComparisonsSequentially(rules);
        }
    }

    /**
     * 异步执行所有比较
     * 
     * @return CompletableFuture<List<ComparisonResult>>
     */
    @Async
    public CompletableFuture<List<ComparisonResult>> executeAllComparisonsAsync() {
        return CompletableFuture.completedFuture(executeAllComparisons());
    }

    /**
     * 并行执行比较
     * 
     * @param rules 规则列表
     * @return List<ComparisonResult>
     */
    private List<ComparisonResult> executeComparisonsInParallel(List<ComparisonProperties.ComparisonRule> rules) {
        logger.info("开始并行执行 {} 个比较规则", rules.size());
        
        List<CompletableFuture<ComparisonResult>> futures = rules.stream()
                .map(rule -> CompletableFuture.supplyAsync(() -> performComparison(rule)))
                .collect(Collectors.toList());
        
        return futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }

    /**
     * 串行执行比较
     * 
     * @param rules 规则列表
     * @return List<ComparisonResult>
     */
    private List<ComparisonResult> executeComparisonsSequentially(List<ComparisonProperties.ComparisonRule> rules) {
        logger.info("开始串行执行 {} 个比较规则", rules.size());
        
        return rules.stream()
                .map(this::performComparison)
                .collect(Collectors.toList());
    }

    /**
     * 执行具体的比较逻辑
     * 
     * @param rule 比较规则
     * @return ComparisonResult
     */
    private ComparisonResult performComparison(ComparisonProperties.ComparisonRule rule) {
        ComparisonResult result = new ComparisonResult(rule.getName());
        result.setRuleDescription(rule.getDescription());
        
        try {
            logger.info("开始执行比较规则: {}", rule.getName());
            
            // 获取JdbcTemplate
            JdbcTemplate sourceJdbcTemplate = dataSourceConfig.getJdbcTemplate(rule.getSourceTable().getDataSource());
            JdbcTemplate targetJdbcTemplate = dataSourceConfig.getJdbcTemplate(rule.getTargetTable().getDataSource());
            
            // 构建查询SQL
            String sourceQuery = buildQuery(rule.getSourceTable(), rule.getKeyField(), rule.getCompareField(), rule.getWhereCondition());
            String targetQuery = buildQuery(rule.getTargetTable(), rule.getKeyField(), rule.getCompareField(), rule.getWhereCondition());
            
            // 查询数据
            Map<Object, Object> sourceData = queryData(sourceJdbcTemplate, sourceQuery);
            Map<Object, Object> targetData = queryData(targetJdbcTemplate, targetQuery);
            
            // 比较数据
            List<DifferenceDetail> differences = compareData(sourceData, targetData, rule.getCompareField());
            
            // 设置结果
            result.setTotalRecords(Math.max(sourceData.size(), targetData.size()));
            result.setDifferences(differences);
            result.calculateStatistics();
            result.setEndTime(LocalDateTime.now());
            result.setStatus(ComparisonResult.ExecutionStatus.SUCCESS);
            
            logger.info("比较规则 {} 执行完成，发现 {} 个差异", rule.getName(), differences.size());
            
        } catch (Exception e) {
            logger.error("执行比较规则 {} 时发生错误", rule.getName(), e);
            result.setStatus(ComparisonResult.ExecutionStatus.FAILED);
            result.setErrorMessage(e.getMessage());
            result.setEndTime(LocalDateTime.now());
        }
        
        return result;
    }

    /**
     * 构建查询SQL
     * 
     * @param tableConfig 表配置
     * @param keyField 主键字段
     * @param compareField 比较字段
     * @param whereCondition WHERE条件
     * @return SQL语句
     */
    private String buildQuery(ComparisonProperties.TableConfig tableConfig, String keyField, String compareField, String whereCondition) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(keyField).append(", ").append(compareField);
        sql.append(" FROM ").append(tableConfig.getFullTableName());
        
        if (whereCondition != null && !whereCondition.trim().isEmpty()) {
            sql.append(" WHERE ").append(whereCondition);
        }
        
        return sql.toString();
    }

    /**
     * 查询数据
     * 
     * @param jdbcTemplate JdbcTemplate
     * @param query 查询SQL
     * @return Map<Object, Object> 键值对数据
     */
    private Map<Object, Object> queryData(JdbcTemplate jdbcTemplate, String query) {
        Map<Object, Object> dataMap = new ConcurrentHashMap<>();
        
        jdbcTemplate.query(query, rs -> {
            Object key = rs.getObject(1);
            Object value = rs.getObject(2);
            dataMap.put(key, value);
        });
        
        return dataMap;
    }

    /**
     * 比较数据
     * 
     * @param sourceData 源数据
     * @param targetData 目标数据
     * @param compareField 比较字段
     * @return List<DifferenceDetail> 差异列表
     */
    private List<DifferenceDetail> compareData(Map<Object, Object> sourceData, 
                                             Map<Object, Object> targetData, 
                                             String compareField) {
        List<DifferenceDetail> differences = new ArrayList<>();
        
        // 找出所有唯一的键
        Set<Object> allKeys = new HashSet<>();
        allKeys.addAll(sourceData.keySet());
        allKeys.addAll(targetData.keySet());
        
        for (Object key : allKeys) {
            Object sourceValue = sourceData.get(key);
            Object targetValue = targetData.get(key);
            
            if (sourceValue == null && targetValue != null) {
                // 仅在目标表存在
                differences.add(new DifferenceDetail(key, DifferenceDetail.DifferenceType.TARGET_ONLY, 
                        null, targetValue, compareField));
            } else if (sourceValue != null && targetValue == null) {
                // 仅在源表存在
                differences.add(new DifferenceDetail(key, DifferenceDetail.DifferenceType.SOURCE_ONLY, 
                        sourceValue, null, compareField));
            } else if (sourceValue != null && targetValue != null) {
                // 都存在，比较值
                if (!Objects.equals(sourceValue, targetValue)) {
                    differences.add(new DifferenceDetail(key, DifferenceDetail.DifferenceType.VALUE_DIFFERENT, 
                            sourceValue, targetValue, compareField));
                }
            }
        }
        
        return differences;
    }

    /**
     * 获取启用的规则列表
     * 
     * @return List<ComparisonRule>
     */
    private List<ComparisonProperties.ComparisonRule> getEnabledRules() {
        if (comparisonProperties.getRules() == null) {
            return Collections.emptyList();
        }
        
        return comparisonProperties.getRules().stream()
                .filter(ComparisonProperties.ComparisonRule::isEnabled)
                .collect(Collectors.toList());
    }

    /**
     * 根据名称查找规则
     * 
     * @param ruleName 规则名称
     * @return ComparisonRule
     */
    private ComparisonProperties.ComparisonRule findRuleByName(String ruleName) {
        if (comparisonProperties.getRules() == null) {
            return null;
        }
        
        return comparisonProperties.getRules().stream()
                .filter(rule -> ruleName.equals(rule.getName()))
                .findFirst()
                .orElse(null);
    }

    /**
     * 验证数据源连接
     * 
     * @return Map<String, Boolean> 数据源名称 -> 连接状态
     */
    public Map<String, Boolean> validateDataSourceConnections() {
        Map<String, Boolean> connectionStatus = new HashMap<>();
        
        for (String dataSourceName : dataSourceConfig.getAllDataSourceNames()) {
            boolean isConnected = dataSourceConfig.testConnection(dataSourceName);
            connectionStatus.put(dataSourceName, isConnected);
            
            if (isConnected) {
                logger.info("数据源 {} 连接正常", dataSourceName);
            } else {
                logger.warn("数据源 {} 连接失败", dataSourceName);
            }
        }
        
        return connectionStatus;
    }

    /**
     * 获取所有规则的基本信息
     * 
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> getAllRulesInfo() {
        if (comparisonProperties.getRules() == null) {
            return Collections.emptyList();
        }
        
        return comparisonProperties.getRules().stream()
                .map(rule -> {
                    Map<String, Object> info = new HashMap<>();
                    info.put("name", rule.getName());
                    info.put("description", rule.getDescription());
                    info.put("enabled", rule.isEnabled());
                    info.put("sourceTable", rule.getSourceTable().getFullTableName());
                    info.put("targetTable", rule.getTargetTable().getFullTableName());
                    info.put("compareField", rule.getCompareField());
                    info.put("keyField", rule.getKeyField());
                    return info;
                })
                .collect(Collectors.toList());
    }
}
