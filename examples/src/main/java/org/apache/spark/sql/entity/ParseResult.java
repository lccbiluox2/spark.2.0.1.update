package org.apache.spark.sql.entity;


import java.util.LinkedList;
import java.util.List;

/**
 * Created by lcc on 2018/7/5.
 */

public class ParseResult {

    /**
     * 输入列表
     */
    private List<Entity> inputList;

    /**
     * 输出列表
     */
    private List<Entity> outputList;


    public ParseResult() {
        this.inputList = new LinkedList<>();
        this.outputList = new LinkedList<>();
    }
}
