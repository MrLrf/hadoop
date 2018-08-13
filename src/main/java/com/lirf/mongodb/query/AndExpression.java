package com.lirf.mongodb.query;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/8 8:56
 */
public class AndExpression implements ConditionExpression {

    private List<ConditionExpression> items = new ArrayList<>();
    private ConditionExpressionFactory expressionFactory;

    @Override
    public void deSerialize(String expressionJson) {
        items.clear();
        JSONObject jobj = new JSONObject(expressionJson);
        JSONArray itemsArray = jobj.getJSONArray("items");
        for(int i=0;i<itemsArray.length();i++){
            JSONObject itemjson = itemsArray.getJSONObject(i);
            ConditionExpression exp = expressionFactory.createConditionExpression(itemjson.toString());
            items.add(exp);
        }
    }

    @Override
    public Bson toBson() {
        List<Bson> bsonlist = new ArrayList<>();
        for(ConditionExpression exp:items){
            bsonlist.add(exp.toBson());
        }
        return Filters.and(bsonlist.toArray(new Bson[0]));
    }

    @Override
    public void setExpressionFactory(ConditionExpressionFactory expressionFactory) {
        this.expressionFactory = expressionFactory;
    }
}
