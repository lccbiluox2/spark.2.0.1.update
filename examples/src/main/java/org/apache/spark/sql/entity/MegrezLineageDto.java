package org.apache.spark.sql.entity;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lcc on 2018/7/5.
 * Hive 血缘Logger 分析结果类
 * @author hulb
 * @date 2017/12/19 下午4:15
 *
 *
 *  in_FIL(0)  in_FIL(1) in_FIL(2)  in_FIL(3)
 *         \    /         \    /
 *       out_FIL(0)      out_FIL(1)
 *
 * 顶点为 FIL(0), FIL(1), FIL(2)
 * 边为：(
 *          in_FIL[0 ,1] -> out_[0],
 *          in_FIL[2 ,3] -> out_[1]
 *      )
 *
 *
 */
public class MegrezLineageDto implements Serializable {

    String user;
    /**所属数据库*/
    String database;
    /**查询内容sql*/
    String queryText;
    /**边点*/
    List<MegrezLineageEdge> edges;

    public MegrezLineageDto(){
        edges = new ArrayList<>();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    public List<MegrezLineageEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<MegrezLineageEdge> edges) {
        this.edges = edges;
    }
}
