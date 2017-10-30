package com.lirf.mongodb.query;


import org.bson.conversions.Bson;

/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/8 8:55
 */
public interface ConditionExpression {

    void setExpressionFactory(ConditionExpressionFactory expressionFactory);

    void deSerialize(String expressionJson);

    Bson toBson();

}
