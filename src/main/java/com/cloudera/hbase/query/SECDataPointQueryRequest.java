package com.cloudera.hbase.query;

/**
 * Created by root on 6/11/17.
 */
public class SECDataPointQueryRequest extends HBaseQueryRequest{
    private String pointCode;
    private String factoryCode;
    private String groupCode;
    private Long startTime;
    private Long endTime;
    private int type;


    public SECDataPointQueryRequest(String pointCode, String factoryCode, String groupCode, Long startTime, Long endTime) {
        this.pointCode = pointCode;
        this.factoryCode = factoryCode;
        this.groupCode = groupCode;
        this.startTime = startTime;
        this.endTime = endTime;
    }


    public String getPointCode() {
        return pointCode;
    }

    public void setPointCode(String pointCode) {
        this.pointCode = pointCode;
    }

    public String getFactoryCode() {
        return factoryCode;
    }

    public void setFactoryCode(String factoryCode) {
        this.factoryCode = factoryCode;
    }

    public String getGroupCode() {
        return groupCode;
    }

    public void setGroupCode(String groupCode) {
        this.groupCode = groupCode;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "SECDataPointQuerRequest{" +
                "pointCode='" + pointCode + '\'' +
                ", factoryCode='" + factoryCode + '\'' +
                ", groupCode='" + groupCode + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}

