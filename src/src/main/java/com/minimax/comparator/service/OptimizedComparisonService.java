package com.minimax.comparator.service;

import com.minimax.comparator.config.ComparisonProperties;
import com.minimax.comparator.config.DynamicDataSourceConfig;
import com.minimax.comparator.model.ComparisonResult;
import com.minimax.comparator.model.DifferenceDetail;
import com.minimax.comparator.util.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 优化的比较服务类
 * 针对大数据量场景进行性能优化
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@Service
public class OptimizedComparisonService {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedComparisonService.class);

    @Autowired
    private ComparisonProperties comparisonProperties;

    @Autowired
    private DynamicDataSourceConfig dataSourceConfig;

    /**
     * 大数据量优化比较
     * 
     * @param rule 比较规则
     * @return ComparisonResult
     */
    public ComparisonResult performOptimizedComparison(ComparisonProperties.ComparisonRule rule) {
        ComparisonResult result = new ComparisonResult(rule.getName());
        result.setRuleDescription(rule.getDescription());
        
        try {
            logger.info("开始执行优化比较规则: {}", rule.getName());
            
            // 获取JdbcTemplate
            JdbcTemplate sourceJdbcTemplate = dataSourceConfig.getJdbcTemplate(rule.getSourceTable().getDataSource());
            JdbcTemplate targetJdbcTemplate = dataSourceConfig.getJdbcTemplate(rule.getTargetTable().getDataSource());
            
            // 检查表和字段是否存在
            validateTableAndFields(sourceJdbcTemplate, targetJdbcTemplate, rule);
            
            // 获取总记录数
            long sourceTotalCount = getTotalCount(sourceJdbcTemplate, rule.getSourceTable(), rule.getWhereCondition());
            long targetTotalCount = getTotalCount(targetJdbcTemplate, rule.getTargetTable(), rule.getWhereCondition());
            
            result.setTotalRecords(Math.max(sourceTotalCount, targetTotalCount));
            
            // 选择比较策略
            List<DifferenceDetail> differences;
            if (sourceTotalCount <= comparisonProperties.getBatchSize() && 
                targetTotalCount <= comparisonProperties.getBatchSize()) {
                // 小数据量，直接比较
                differences = performDirectComparison(sourceJdbcTemplate, targetJdbcTemplate, rule);
            } else {
                // 大数据量，分批比较
                differences = performBatchComparison(sourceJdbcTemplate, targetJdbcTemplate, rule);
            }
            
            // 设置结果
            result.setDifferences(differences);
            result.calculateStatistics();
            result.setEndTime(LocalDateTime.now());
            result.setStatus(ComparisonResult.ExecutionStatus.SUCCESS);
            
            logger.info("优化比较规则 {} 执行完成，发现 {} 个差异", rule.getName(), differences.size());
            
        } catch (Exception e) {
            logger.error("执行优化比较规则 {} 时发生错误", rule.getName(), e);
            result.setStatus(ComparisonResult.ExecutionStatus.FAILED);
            result.setErrorMessage(e.getMessage());
            result.setEndTime(LocalDateTime.now());
        }
        
        return result;
    }

    /**
     * 验证表和字段是否存在
     */
    private void validateTableAndFields(JdbcTemplate sourceJdbcTemplate, 
                                      JdbcTemplate targetJdbcTemplate, 
                                      ComparisonProperties.ComparisonRule rule) {
        // 这里可以添加表和字段存在性验证逻辑
        // 为了简化示例，暂时跳过
        logger.debug("验证表和字段存在性: {}", rule.getName());
    }

    /**
     * 获取总记录数
     */
    private long getTotalCount(JdbcTemplate jdbcTemplate, 
                             ComparisonProperties.TableConfig tableConfig, 
                             String whereCondition) {
        String countSql = SqlBuilder.buildCountSql(tableConfig, whereCondition);
        Long count = jdbcTemplate.queryForObject(countSql, Long.class);
        return count != null ? count : 0L;
    }

    /**
     * 直接比较（小数据量）
     */
    private List<DifferenceDetail> performDirectComparison(JdbcTemplate sourceJdbcTemplate,
                                                         JdbcTemplate targetJdbcTemplate,
                                                         ComparisonProperties.ComparisonRule rule) {
        logger.debug("执行直接比较策略");
        
        // 构建查询SQL
        String sourceQuery = buildQuery(rule.getSourceTable(), rule.getKeyField(), rule.getCompareField(), rule.getWhereCondition());
        String targetQuery = buildQuery(rule.getTargetTable(), rule.getKeyField(), rule.getCompareField(), rule.getWhereCondition());
        
        // 查询数据
        Map<Object, Object> sourceData = queryData(sourceJdbcTemplate, sourceQuery);
        Map<Object, Object> targetData = queryData(targetJdbcTemplate, targetQuery);
        
        // 比较数据
        return compareData(sourceData, targetData, rule.getCompareField());
    }

    /**
     * 分批比较（大数据量）
     */
    private List<DifferenceDetail> performBatchComparison(JdbcTemplate sourceJdbcTemplate,
                                                        JdbcTemplate targetJdbcTemplate,
                                                        ComparisonProperties.ComparisonRule rule) {
        logger.debug("执行分批比较策略");
        
        List<DifferenceDetail> allDifferences = new ArrayList<>();
        int batchSize = comparisonProperties.getBatchSize();
        int offset = 0;
        
        // 获取数据库类型（简化处理，实际应该从配置中获取）
        SqlBuilder.DatabaseType sourceDbType = SqlBuilder.DatabaseType.MYSQL;
        SqlBuilder.DatabaseType targetDbType = SqlBuilder.DatabaseType.MYSQL;
        
        while (true) {
            // 分批查询源数据
            String sourceQuery = SqlBuilder.buildPagedSelectSql(
                    rule.getSourceTable(),
                    rule.getKeyField() + ", " + rule.getCompareField(),
                    rule.getWhereCondition(),
                    rule.getKeyField(),
                    offset,
                    batchSize,
                    sourceDbType
            );
            
            Map<Object, Object> sourceBatch = queryData(sourceJdbcTemplate, sourceQuery);
            if (sourceBatch.isEmpty()) {
                break;
            }
            
            // 获取当前批次的主键列表
            Set<Object> keySet = sourceBatch.keySet();
            
            // 查询目标数据中对应的记录
            Map<Object, Object> targetBatch = queryDataByKeys(targetJdbcTemplate, rule.getTargetTable(), 
                    rule.getKeyField(), rule.getCompareField(), keySet, rule.getWhereCondition());
            
            // 比较当前批次
            List<DifferenceDetail> batchDifferences = compareData(sourceBatch, targetBatch, rule.getCompareField());
            allDifferences.addAll(batchDifferences);
            
            logger.debug("完成批次比较，偏移量: {}, 差异数: {}", offset, batchDifferences.size());
            
            offset += batchSize;
            
            // 避免无限循环
            if (sourceBatch.size() < batchSize) {
                break;
            }
        }
        
        // 处理仅在目标表中存在的记录
        allDifferences.addAll(findTargetOnlyRecords(sourceJdbcTemplate, targetJdbcTemplate, rule));
        
        return allDifferences;
    }

    /**
     * 根据主键列表查询数据
     */
    private Map<Object, Object> queryDataByKeys(JdbcTemplate jdbcTemplate,
                                              ComparisonProperties.TableConfig tableConfig,
                                              String keyField,
                                              String compareField,
                                              Set<Object> keys,
                                              String whereCondition) {
        if (keys.isEmpty()) {
            return new HashMap<>();
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(keyField).append(", ").append(compareField);
        sql.append(" FROM ").append(tableConfig.getFullTableName());
        sql.append(" WHERE ").append(SqlBuilder.buildInCondition(keyField, keys.size()));
        
        if (whereCondition != null && !whereCondition.trim().isEmpty()) {
            sql.append(" AND (").append(whereCondition).append(")");
        }
        
        Map<Object, Object> dataMap = new ConcurrentHashMap<>();
        List<Object> keyList = new ArrayList<>(keys);
        
        jdbcTemplate.query(sql.toString(), keyList.toArray(), rs -> {
            Object key = rs.getObject(1);
            Object value = rs.getObject(2);
            dataMap.put(key, value);
        });
        
        return dataMap;
    }

    /**
     * 查找仅在目标表中存在的记录
     */
    private List<DifferenceDetail> findTargetOnlyRecords(JdbcTemplate sourceJdbcTemplate,
                                                        JdbcTemplate targetJdbcTemplate,
                                                        ComparisonProperties.ComparisonRule rule) {
        List<DifferenceDetail> targetOnlyDifferences = new ArrayList<>();
        
        // 获取所有源表的主键
        String sourceKeysQuery = SqlBuilder.buildSelectSql(rule.getSourceTable(), rule.getKeyField(), rule.getWhereCondition());
        Set<Object> sourceKeys = new HashSet<>();
        sourceJdbcTemplate.query(sourceKeysQuery, rs -> {
            sourceKeys.add(rs.getObject(1));
        });
        
        // 分批处理目标表数据
        int batchSize = comparisonProperties.getBatchSize();
        int offset = 0;
        SqlBuilder.DatabaseType targetDbType = SqlBuilder.DatabaseType.MYSQL;
        
        while (true) {
            String targetQuery = SqlBuilder.buildPagedSelectSql(
                    rule.getTargetTable(),
                    rule.getKeyField() + ", " + rule.getCompareField(),
                    rule.getWhereCondition(),
                    rule.getKeyField(),
                    offset,
                    batchSize,
                    targetDbType
            );
            
            List<DifferenceDetail> batchTargetOnly = new ArrayList<>();
            targetJdbcTemplate.query(targetQuery, rs -> {
                Object key = rs.getObject(1);
                Object value = rs.getObject(2);
                
                if (!sourceKeys.contains(key)) {
                    batchTargetOnly.add(new DifferenceDetail(key, DifferenceDetail.DifferenceType.TARGET_ONLY, 
                            null, value, rule.getCompareField()));
                }
            });
            
            targetOnlyDifferences.addAll(batchTargetOnly);
            
            if (batchTargetOnly.size() < batchSize) {
                break;
            }
            
            offset += batchSize;
        }
        
        return targetOnlyDifferences;
    }

    /**
     * 构建查询SQL
     */
    private String buildQuery(ComparisonProperties.TableConfig tableConfig, String keyField, String compareField, String whereCondition) {
        return SqlBuilder.buildSelectSql(tableConfig, keyField + ", " + compareField, whereCondition);
    }

    /**
     * 查询数据
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
     * 获取数据库统计信息
     */
    public Map<String, Object> getDatabaseStatistics(String dataSourceName) {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            JdbcTemplate jdbcTemplate = dataSourceConfig.getJdbcTemplate(dataSourceName);
            
            // 获取数据库基本信息
            String version = jdbcTemplate.queryForObject("SELECT VERSION()", String.class);
            stats.put("version", version);
            
            // 获取当前时间
            String currentTime = jdbcTemplate.queryForObject("SELECT NOW()", String.class);
            stats.put("currentTime", currentTime);
            
            stats.put("dataSourceName", dataSourceName);
            stats.put("connected", true);
            
        } catch (Exception e) {
            stats.put("connected", false);
            stats.put("error", e.getMessage());
        }
        
        return stats;
    }
}
