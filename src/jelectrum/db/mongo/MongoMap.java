package jelectrum.db.mongo;

import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.Collection;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.Random;
import java.text.DecimalFormat;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import jelectrum.db.DBMap;
import com.google.protobuf.ByteString;


public class MongoMap extends DBMap
{
    private DBCollection collection;

    public MongoMap(DBCollection collection)
    {
        this.collection = collection;
    }

    public void clear()
    {
        collection.drop();
    }


    public boolean containsKey(String key)
    {
        boolean c = (collection.count(new MongoKey(key))>0);
        return c;
    }

    public ByteString get(String key)
    {
      DBObject o = collection.findOne(new MongoKey(key));
      if (o==null) return null;

      return MongoEntry.getValueByte(o);

    }

    public void put(String key, ByteString value) 
    {
        collection.save(new MongoEntry(key, value), WriteConcern.ACKNOWLEDGED);
    }
    public void putAll(Map<String, ByteString> m) 
    {
      try
      {
        LinkedList<MongoEntry> lst = new LinkedList<MongoEntry>();
    
        for(Map.Entry<String, ByteString> me : m.entrySet())
        {
          lst.add(new MongoEntry(me.getKey(), me.getValue()));
        }
        collection.insert(lst, WriteConcern.ACKNOWLEDGED);
      }
      catch(com.mongodb.DuplicateKeyException e)
      {
        for(Map.Entry<String,ByteString> me : m.entrySet())
        {
          put(me.getKey(), me.getValue());
        }

      }
    }



}
