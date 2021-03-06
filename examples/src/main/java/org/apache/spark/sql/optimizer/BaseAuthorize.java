package org.apache.spark.sql.optimizer;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * 对外暴露的基础的鉴权父类
 * 作为父类 只有空的实现 依赖外部指定className做具体的鉴权逻辑
 */
public interface BaseAuthorize {
    /**
     * 权限校验接口
     *
     * @param plan     逻辑计划
     * @param userId   用户ID
     * @param tenantId 租户ID
     */
    void check(LogicalPlan plan, String userId, String tenantId);
}
