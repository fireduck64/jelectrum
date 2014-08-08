package jelectrum;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
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
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import com.mongodb.BasicDBObject;

public class MongoMapSet<K,V>
{
    private DBCollection collection;
    private boolean compress;

    private static StatData put_stats = new StatData();
    private static StatData get_stats = new StatData();

    public static final String KEY="key";


    public MongoMapSet(DBCollection collection, boolean compress)
    {
        this.collection = collection;
        this.compress=compress;

        collection.ensureIndex(new BasicDBObject(KEY,1), new BasicDBObject());
        /*Map<String, Object> m = new TreeMap<String, Object>();
        m.put(KEY,1);
        m.put("data",1);
        collection.ensureIndex(new BasicDBObject(m), new BasicDBObject("unique",true));*/
    }

    public void clear()
    {
        collection.drop();
    }

    public boolean containsKey(Object key)
    {
        long t1 = System.currentTimeMillis();
        boolean c = (collection.count(new BasicDBObject(KEY,key.toString()))>0);
        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return c;
    }
    public long countKey(Object key)
    {
        long t1 = System.currentTimeMillis();
        long c = collection.count(new BasicDBObject(KEY,key.toString()));
        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return c;
    }


    public Set<V> getSet(Object key)
    {
        Set<V> ret = new HashSet<V>();

        long t1 = System.currentTimeMillis();
        DBCursor c = collection.find(new BasicDBObject(KEY, key.toString()));
        while(c.hasNext())
        {
            ret.add((V)MongoEntry.getValue(c.next(), compress));
        }
        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);

        return ret;


    }

    public boolean isEmpty()
    {
        return (size()==0);
    }
    public int size()
    {
        return (int)collection.count();
    }

    public void add(K key, V value) 
    {
        //V old = get(key);
        long t1 = System.currentTimeMillis();
        collection.save(new MongoEntry(key.toString(), value, compress, KEY), WriteConcern.ACKNOWLEDGED);
        long t2 = System.currentTimeMillis();
        put_stats.addDataPoint(t2-t1);
        //return old;
    }

    public static void printStats()
    {
        DecimalFormat df = new DecimalFormat("0.0");

        get_stats.copyAndReset().print("get", df);
        put_stats.copyAndReset().print("put", df);

    }



}
