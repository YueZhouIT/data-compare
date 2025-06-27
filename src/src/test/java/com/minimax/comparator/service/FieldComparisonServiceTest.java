package com.minimax.comparator.service;

import com.minimax.comparator.config.ComparisonProperties;
import com.minimax.comparator.config.DynamicDataSourceConfig;
import com.minimax.comparator.model.ComparisonResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 字段比较服务测试类
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@ExtendWith(MockitoExtension.class)
class FieldComparisonServiceTest {

    @Mock
    private ComparisonProperties comparisonProperties;

    @Mock
    private DynamicDataSourceConfig dataSourceConfig;

    @Mock
    private JdbcTemplate sourceJdbcTemplate;

    @Mock
    private JdbcTemplate targetJdbcTemplate;

    @InjectMocks
    private FieldComparisonService fieldComparisonService;

    private ComparisonProperties.ComparisonRule testRule;

    @BeforeEach
    void setUp() {
        // 创建测试规则
        testRule = new ComparisonProperties.ComparisonRule();
        testRule.setName("test-rule");
        testRule.setDescription("测试规则");
        testRule.setEnabled(true);
        testRule.setKeyField("id");
        testRule.setCompareField("name");
        
        // 创建源表配置
        ComparisonProperties.TableConfig sourceTable = new ComparisonProperties.TableConfig();
        sourceTable.setDataSource("source-db");
        sourceTable.setTableName("users");
        testRule.setSourceTable(sourceTable);
        
        // 创建目标表配置
        ComparisonProperties.TableConfig targetTable = new ComparisonProperties.TableConfig();
        targetTable.setDataSource("target-db");
        targetTable.setTableName("users");
        testRule.setTargetTable(targetTable);
    }

    @Test
    void testExecuteComparison_Success() {
        // 配置mock
        when(comparisonProperties.getRules()).thenReturn(Arrays.asList(testRule));
        when(dataSourceConfig.getJdbcTemplate("source-db")).thenReturn(sourceJdbcTemplate);
        when(dataSourceConfig.getJdbcTemplate("target-db")).thenReturn(targetJdbcTemplate);
        
        // 模拟数据查询
        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(1);
            // 模拟源数据
            return null;
        }).when(sourceJdbcTemplate).query(anyString(), any(RowCallbackHandler.class));
        
        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(1);
            // 模拟目标数据
            return null;
        }).when(targetJdbcTemplate).query(anyString(), any(RowCallbackHandler.class));
        
        // 执行测试
        ComparisonResult result = fieldComparisonService.executeComparison("test-rule");
        
        // 验证结果
        assertNotNull(result);
        assertEquals("test-rule", result.getRuleName());
        assertEquals("测试规则", result.getRuleDescription());
        assertEquals(ComparisonResult.ExecutionStatus.SUCCESS, result.getStatus());
    }

    @Test
    void testExecuteComparison_RuleNotFound() {
        // 配置mock
        when(comparisonProperties.getRules()).thenReturn(Arrays.asList());
        
        // 执行测试并验证异常
        assertThrows(IllegalArgumentException.class, () -> {
            fieldComparisonService.executeComparison("non-existent-rule");
        });
    }

    @Test
    void testGetAllRulesInfo() {
        // 配置mock
        when(comparisonProperties.getRules()).thenReturn(Arrays.asList(testRule));
        
        // 执行测试
        List<java.util.Map<String, Object>> rulesInfo = fieldComparisonService.getAllRulesInfo();
        
        // 验证结果
        assertNotNull(rulesInfo);
        assertEquals(1, rulesInfo.size());
        
        java.util.Map<String, Object> ruleInfo = rulesInfo.get(0);
        assertEquals("test-rule", ruleInfo.get("name"));
        assertEquals("测试规则", ruleInfo.get("description"));
        assertEquals(true, ruleInfo.get("enabled"));
    }

    @Test
    void testValidateDataSourceConnections() {
        // 配置mock
        when(dataSourceConfig.getAllDataSourceNames()).thenReturn(java.util.Set.of("source-db", "target-db"));
        when(dataSourceConfig.testConnection("source-db")).thenReturn(true);
        when(dataSourceConfig.testConnection("target-db")).thenReturn(false);
        
        // 执行测试
        java.util.Map<String, Boolean> connectionStatus = fieldComparisonService.validateDataSourceConnections();
        
        // 验证结果
        assertNotNull(connectionStatus);
        assertEquals(2, connectionStatus.size());
        assertTrue(connectionStatus.get("source-db"));
        assertFalse(connectionStatus.get("target-db"));
    }
}
