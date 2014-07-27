package jelectrum;

import java.util.Map;
import java.util.Set;
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

public class MongoMap<K,V> implements Map<K, V>
{
    private DBCollection collection;
    private boolean compress;

    private static StatData put_stats = new StatData();
    private static StatData get_stats = new StatData();



    public MongoMap(DBCollection collection, boolean compress)
    {
        this.collection = collection;
        this.compress=compress;
    }

    public void clear()
    {
        collection.drop();
    }


    public boolean containsKey(Object key)
    {
        long t1 = System.currentTimeMillis();
        boolean c = (collection.count(new MongoKey(key.toString()))>0);
        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        return c;
    }
    public boolean containsValue(Object value)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public  boolean equals(Object o)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V get(Object key)
    {
        long t1 = System.currentTimeMillis();
        DBObject o = collection.findOne(new MongoKey(key.toString()));
        long t2 = System.currentTimeMillis();
        get_stats.addDataPoint(t2-t1);
        if (o==null) return null;

        return (V) MongoEntry.getValue(o, compress);


    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public boolean isEmpty()
    {
        return (size()==0);
    }
    public int size()
    {
        return (int)collection.count();
    }


    public  Set<K>  keySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value) 
    {
        //V old = get(key);
        long t1 = System.currentTimeMillis();
        collection.save(new MongoEntry(key.toString(), value, compress), WriteConcern.ACKNOWLEDGED);
        long t2 = System.currentTimeMillis();
        put_stats.addDataPoint(t2-t1);
        //return old;
        return null;
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V remove(Object key) 
    {
        throw new RuntimeException("not implemented - is stupid");

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public static void printStats()
    {
        DecimalFormat df = new DecimalFormat("0.0");

        get_stats.copyAndReset().print("get", df);
        put_stats.copyAndReset().print("put", df);

    }



}
