package jelectrum;

import com.mongodb.BasicDBObject;

public class MongoKey extends BasicDBObject
{
    public MongoKey(String key)
    {
        super("_id", key);
    }


    public String getKey()
    {
        return getString("_id");
    }
}
