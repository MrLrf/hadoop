package com.lirf.mongodb.query;

import org.json.JSONObject;

/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/8 9:05
 */
public class DefaultConditionExpressionFactory implements ConditionExpressionFactory {
    @Override
    public ConditionExpression createConditionExpression(String expressionJson) {
        JSONObject jsonObject = new JSONObject(expressionJson);
        String type = jsonObject.get("type").toString();
        ConditionExpression exp = null;
        try {
            exp = (ConditionExpression) Class.forName(type).newInstance();
            exp.setExpressionFactory(new DefaultConditionExpressionFactory());
            exp.deSerialize(expressionJson);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return exp;
    }
}
