package org.apache.spark.sql.entity;


import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by lcc on 2018/7/5.
 * @notice  java和scala混合语言的时候，需要手动书写getter和setter方法，不然会出现问题
 */
public class LineageParseResult extends ParseResult implements Serializable {

    private Long userId;
    private Long tenantId;
    private Long workspaceId;
    private String schemaName;
    private String optType;
    private String command;
    private MegrezLineageDto megrezLineageDto;

    /**
     * 输入列表
     */
    private List<Entity> inputList;

    /**
     * 所有的字段集合，格式：数据库名.表名.字段名
     */
    private List<String> fieldList;
    private List<String> functionList;
    /**
     * 输出列表
     */
    private List<Entity> outputList;


    public LineageParseResult() {
        this.inputList = new LinkedList<>();
        this.functionList = new LinkedList<>();
        this.outputList = new LinkedList<>();
        this.fieldList = new LinkedList<>();
    }

    @Override
    public String toString() {
        return "LineageParseResult{" +"\n"+
                "userId=" + userId +"\n"+
                ", tenantId=" + tenantId +"\n"+
                ", workspaceId=" + workspaceId +"\n"+
                ", schemaName='" + schemaName + '\'' +"\n"+
                ", optType='" + optType + '\'' +"\n"+
                ", command='" + command + '\'' +"\n"+
                ", megrezLineageDto=" + megrezLineageDto +"\n"+
                ", inputList=" + inputList +"\n"+
                ", fieldList=" + fieldList +"\n"+
                ", outputList=" + outputList +"\n"+
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Long getWorkspaceId() {
        return workspaceId;
    }

    public void setWorkspaceId(Long workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getOptType() {
        return optType;
    }

    public void setOptType(String optType) {
        this.optType = optType;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public MegrezLineageDto getMegrezLineageDto() {
        return megrezLineageDto;
    }

    public void setMegrezLineageDto(MegrezLineageDto megrezLineageDto) {
        this.megrezLineageDto = megrezLineageDto;
    }

    public List<Entity> getInputList() {
        return inputList;
    }

    public void setInputList(List<Entity> inputList) {
        this.inputList = inputList;
    }

    public List<String> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<String> fieldList) {
        this.fieldList = fieldList;
    }

    public List<String> getFunctionList() {
        return functionList;
    }

    public void setFunctionList(List<String> functionList) {
        this.functionList = functionList;
    }

    public List<Entity> getOutputList() {
        return outputList;
    }

    public void setOutputList(List<Entity> outputList) {
        this.outputList = outputList;
    }
}
