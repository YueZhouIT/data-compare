package com.minimax.comparator.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 比较结果实体类
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
public class ComparisonResult {
    
    /**
     * 规则名称
     */
    private String ruleName;
    
    /**
     * 规则描述
     */
    private String ruleDescription;
    
    /**
     * 比较开始时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime startTime;
    
    /**
     * 比较结束时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime endTime;
    
    /**
     * 执行耗时（毫秒）
     */
    private long executionTime;
    
    /**
     * 总记录数
     */
    private long totalRecords;
    
    /**
     * 差异记录数
     */
    private long differenceCount;
    
    /**
     * 仅在源表存在的记录数
     */
    private long sourceOnlyCount;
    
    /**
     * 仅在目标表存在的记录数
     */
    private long targetOnlyCount;
    
    /**
     * 值不同的记录数
     */
    private long valueDifferenceCount;
    
    /**
     * 差异详情列表
     */
    private List<DifferenceDetail> differences;
    
    /**
     * 执行状态
     */
    private ExecutionStatus status;
    
    /**
     * 错误信息
     */
    private String errorMessage;

    /**
     * 执行状态枚举
     */
    public enum ExecutionStatus {
        SUCCESS("成功"),
        FAILED("失败"),
        PARTIAL("部分成功");
        
        private final String description;
        
        ExecutionStatus(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }

    // Constructors
    public ComparisonResult() {
        this.startTime = LocalDateTime.now();
        this.status = ExecutionStatus.SUCCESS;
    }

    public ComparisonResult(String ruleName) {
        this();
        this.ruleName = ruleName;
    }

    // Getters and Setters
    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleDescription() {
        return ruleDescription;
    }

    public void setRuleDescription(String ruleDescription) {
        this.ruleDescription = ruleDescription;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
        if (this.startTime != null && endTime != null) {
            this.executionTime = java.time.Duration.between(this.startTime, endTime).toMillis();
        }
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getDifferenceCount() {
        return differenceCount;
    }

    public void setDifferenceCount(long differenceCount) {
        this.differenceCount = differenceCount;
    }

    public long getSourceOnlyCount() {
        return sourceOnlyCount;
    }

    public void setSourceOnlyCount(long sourceOnlyCount) {
        this.sourceOnlyCount = sourceOnlyCount;
    }

    public long getTargetOnlyCount() {
        return targetOnlyCount;
    }

    public void setTargetOnlyCount(long targetOnlyCount) {
        this.targetOnlyCount = targetOnlyCount;
    }

    public long getValueDifferenceCount() {
        return valueDifferenceCount;
    }

    public void setValueDifferenceCount(long valueDifferenceCount) {
        this.valueDifferenceCount = valueDifferenceCount;
    }

    public List<DifferenceDetail> getDifferences() {
        return differences;
    }

    public void setDifferences(List<DifferenceDetail> differences) {
        this.differences = differences;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * 计算差异统计
     */
    public void calculateStatistics() {
        if (differences != null) {
            this.differenceCount = differences.size();
            this.sourceOnlyCount = differences.stream()
                    .mapToLong(d -> DifferenceDetail.DifferenceType.SOURCE_ONLY.equals(d.getType()) ? 1 : 0)
                    .sum();
            this.targetOnlyCount = differences.stream()
                    .mapToLong(d -> DifferenceDetail.DifferenceType.TARGET_ONLY.equals(d.getType()) ? 1 : 0)
                    .sum();
            this.valueDifferenceCount = differences.stream()
                    .mapToLong(d -> DifferenceDetail.DifferenceType.VALUE_DIFFERENT.equals(d.getType()) ? 1 : 0)
                    .sum();
        }
    }

    @Override
    public String toString() {
        return String.format("ComparisonResult{ruleName='%s', status=%s, totalRecords=%d, differenceCount=%d, executionTime=%dms}",
                ruleName, status, totalRecords, differenceCount, executionTime);
    }
}
