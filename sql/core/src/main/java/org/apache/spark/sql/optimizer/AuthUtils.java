package org.apache.spark.sql.optimizer;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author hulb
 * @date 2018/7/9 下午9:11
 */
public class AuthUtils {

    /**
     * 鉴权工具提供的鉴权方法 依赖具体的class实现 由提交脚本提供具体的实现类
     *
     * @param plan      逻辑计划
     * @param userId    用户ID
     * @param tenantId  租户ID
     * @param className 类名
     */
    public static void auth(LogicalPlan plan, String userId, String tenantId, String className) {
        try {
            Class<? extends BaseAuthorize> author = getClazz(className);
            author.newInstance().check(plan, userId, tenantId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Class<? extends BaseAuthorize> getClazz(String clazzName) throws Exception {
        Class<? extends BaseAuthorize> runner;
        try {
            runner = (Class<? extends BaseAuthorize>) Class.forName(clazzName);
        } catch (ClassNotFoundException e) {
            throw e;
        }
        return runner;
    }
}
