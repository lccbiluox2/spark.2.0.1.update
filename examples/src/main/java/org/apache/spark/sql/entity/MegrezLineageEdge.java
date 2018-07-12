package org.apache.spark.sql.entity;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lcc on 2018/7/5.
 */
public class MegrezLineageEdge implements Serializable {

    // 位置类型的字段，比如count(*)
    public static String unknowField = "unknowField";

    /**边source顶点ID*/
    List<String> sources;
    /**边sink顶点ID*/
    List<String> targets;

    public MegrezLineageEdge(){
        sources = new ArrayList<>();
        targets = new ArrayList<>();
    }


    public List<String> getSources() {
        return sources;
    }

    public void setSources(List<String> sources) {
        this.sources = sources;
    }

    public List<String> getTargets() {
        return targets;
    }

    public void setTargets(List<String> targets) {
        this.targets = targets;
    }
}
