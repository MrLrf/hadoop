package com.lirf.mongodb.query;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.json.JSONObject;


/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/8 8:58
 */
public class LteExpression implements ConditionExpression {
    private String fieldName;
    private Object value;
    private ConditionExpressionFactory expressionFactory;

    @Override
    public void deSerialize(String expressionJson) {
        JSONObject jobj = new JSONObject(expressionJson);
        fieldName = jobj.getString("fieldName");
        value = jobj.get("value");
    }

    @Override
    public Bson toBson() {
        return Filters.lte(fieldName,value);
    }

    @Override
    public void setExpressionFactory(ConditionExpressionFactory expressionFactory) {
        this.expressionFactory = expressionFactory;
    }
}
