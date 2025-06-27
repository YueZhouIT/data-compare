package com.minimax.comparator.controller;

import com.minimax.comparator.model.ComparisonResult;
import com.minimax.comparator.service.FieldComparisonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 比较控制器
 * 提供RESTful API接口
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
@RestController
@RequestMapping("/api/comparison")
@CrossOrigin(origins = "*")
public class ComparisonController {

    @Autowired
    private FieldComparisonService fieldComparisonService;

    /**
     * 执行所有启用的比较规则
     * 
     * @return ResponseEntity<List<ComparisonResult>>
     */
    @PostMapping("/execute-all")
    public ResponseEntity<List<ComparisonResult>> executeAllComparisons() {
        try {
            List<ComparisonResult> results = fieldComparisonService.executeAllComparisons();
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 异步执行所有比较规则
     * 
     * @return ResponseEntity<String>
     */
    @PostMapping("/execute-all-async")
    public ResponseEntity<String> executeAllComparisonsAsync() {
        try {
            CompletableFuture<List<ComparisonResult>> future = fieldComparisonService.executeAllComparisonsAsync();
            return ResponseEntity.ok("异步执行已启动，任务ID: " + future.hashCode());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 执行单个比较规则
     * 
     * @param ruleName 规则名称
     * @return ResponseEntity<ComparisonResult>
     */
    @PostMapping("/execute/{ruleName}")
    public ResponseEntity<ComparisonResult> executeComparison(@PathVariable String ruleName) {
        try {
            ComparisonResult result = fieldComparisonService.executeComparison(ruleName);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 执行指定的比较规则列表
     * 
     * @param ruleNames 规则名称列表
     * @return ResponseEntity<List<ComparisonResult>>
     */
    @PostMapping("/execute-batch")
    public ResponseEntity<List<ComparisonResult>> executeComparisons(@RequestBody List<String> ruleNames) {
        try {
            List<ComparisonResult> results = fieldComparisonService.executeComparisons(ruleNames);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取所有规则信息
     * 
     * @return ResponseEntity<List<Map<String, Object>>>
     */
    @GetMapping("/rules")
    public ResponseEntity<List<Map<String, Object>>> getAllRules() {
        try {
            List<Map<String, Object>> rules = fieldComparisonService.getAllRulesInfo();
            return ResponseEntity.ok(rules);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 验证数据源连接
     * 
     * @return ResponseEntity<Map<String, Boolean>>
     */
    @GetMapping("/validate-connections")
    public ResponseEntity<Map<String, Boolean>> validateConnections() {
        try {
            Map<String, Boolean> connectionStatus = fieldComparisonService.validateDataSourceConnections();
            return ResponseEntity.ok(connectionStatus);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 健康检查接口
     * 
     * @return ResponseEntity<Map<String, String>>
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Database Field Comparator",
                "timestamp", java.time.LocalDateTime.now().toString()
        ));
    }
}
