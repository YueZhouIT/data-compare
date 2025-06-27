package com.minimax.comparator.model;

/**
 * 差异详情实体类
 * 
 * @author MiniMax Agent
 * @since 2025-06-27
 */
public class DifferenceDetail {
    
    /**
     * 主键值
     */
    private Object keyValue;
    
    /**
     * 差异类型
     */
    private DifferenceType type;
    
    /**
     * 源表值
     */
    private Object sourceValue;
    
    /**
     * 目标表值
     */
    private Object targetValue;
    
    /**
     * 比较字段名
     */
    private String fieldName;

    /**
     * 差异类型枚举
     */
    public enum DifferenceType {
        SOURCE_ONLY("仅在源表存在"),
        TARGET_ONLY("仅在目标表存在"),
        VALUE_DIFFERENT("值不同");
        
        private final String description;
        
        DifferenceType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }

    // Constructors
    public DifferenceDetail() {}

    public DifferenceDetail(Object keyValue, DifferenceType type, String fieldName) {
        this.keyValue = keyValue;
        this.type = type;
        this.fieldName = fieldName;
    }

    public DifferenceDetail(Object keyValue, DifferenceType type, Object sourceValue, Object targetValue, String fieldName) {
        this.keyValue = keyValue;
        this.type = type;
        this.sourceValue = sourceValue;
        this.targetValue = targetValue;
        this.fieldName = fieldName;
    }

    // Getters and Setters
    public Object getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(Object keyValue) {
        this.keyValue = keyValue;
    }

    public DifferenceType getType() {
        return type;
    }

    public void setType(DifferenceType type) {
        this.type = type;
    }

    public Object getSourceValue() {
        return sourceValue;
    }

    public void setSourceValue(Object sourceValue) {
        this.sourceValue = sourceValue;
    }

    public Object getTargetValue() {
        return targetValue;
    }

    public void setTargetValue(Object targetValue) {
        this.targetValue = targetValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return String.format("DifferenceDetail{keyValue=%s, type=%s, sourceValue=%s, targetValue=%s, fieldName='%s'}",
                keyValue, type, sourceValue, targetValue, fieldName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        DifferenceDetail that = (DifferenceDetail) o;
        
        if (keyValue != null ? !keyValue.equals(that.keyValue) : that.keyValue != null) return false;
        if (type != that.type) return false;
        return fieldName != null ? fieldName.equals(that.fieldName) : that.fieldName == null;
    }

    @Override
    public int hashCode() {
        int result = keyValue != null ? keyValue.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        return result;
    }
}
