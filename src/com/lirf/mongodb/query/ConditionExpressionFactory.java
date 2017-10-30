package com.lirf.mongodb.query;

/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/8 9:05
 */
public interface ConditionExpressionFactory {

    ConditionExpression createConditionExpression(String expressionJson);

}
