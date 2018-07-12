package org.apache.spark.sql.optimizer;

import org.apache.spark.sql.catalyst.optimizer.Authorizer;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author hulb
 * @date 2018/7/10 上午11:19
 */
public class BaseAuthorizeImpl implements BaseAuthorize {

    /**
     * 权限校验接口
     * @param plan
     * @param userId
     * @param tenantId
     */
    @Override
    public void check(LogicalPlan plan, String userId, String tenantId){
        System.out.println("这里是特定的基于接口的鉴权实现：BaseAuthorizeImpl");
        System.out.println("这里是特定的鉴权实现：DefaultAuthorizerImpl");
        System.out.println("这里是特定的鉴权实现：userId"+userId);
        System.out.println("这里是特定的鉴权实现：tenantId"+tenantId);
        Authorizer.check(plan,userId,tenantId);
    }
}
