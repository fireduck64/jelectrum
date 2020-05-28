package jelectrum.db.mongo;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;

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
import org.bitcoinj.core.Sha256Hash;
import jelectrum.db.DBMapSet;
import jelectrum.db.DBTooManyResultsException;

public class MongoMapSet extends DBMapSet
{
    private DBCollection collection;

    public static final String KEY="key";


    public MongoMapSet(DBCollection collection)
    {
        this.collection = collection;

        //collection.ensureIndex(new BasicDBObject(KEY,1), new BasicDBObject());
        collection.createIndex(new BasicDBObject(KEY,1));
        /*Map<String, Object> m = new TreeMap<String, Object>();
        m.put(KEY,1);
        m.put("data",1);
        collection.ensureIndex(new BasicDBObject(m), new BasicDBObject("unique",true));*/
    }

    public void clear()
    {
        collection.drop();
    }


    public Set<Sha256Hash> getSet(String key, int max_results)
    {
        Set<Sha256Hash> ret = new HashSet<Sha256Hash>();

        DBCursor c = collection.find(new BasicDBObject(KEY, key));

        int count =0;

        while(c.hasNext())
        {
          ret.add(new Sha256Hash( MongoEntry.getValueString(c.next())));
          count++;
          if (count > max_results) throw new DBTooManyResultsException();
        }

        return ret;


    }

    public void add(String key, Sha256Hash value) 
    {
        collection.save(new MongoEntry(key, value.toString(), KEY), WriteConcern.ACKNOWLEDGED);
    }
    public void addAll(Collection<Map.Entry<String, Sha256Hash> > in_lst)
    {
      LinkedList<MongoEntry> lst = new LinkedList<MongoEntry>();

      for(Map.Entry<String, Sha256Hash> me : in_lst)
      {
        lst.add(new MongoEntry(me.getKey(), me.getValue().toString(), KEY));
      }

      if (lst.size() > 0)
      {

        try
        { 

          collection.insert(lst, WriteConcern.ACKNOWLEDGED);
        }
        catch(com.mongodb.DuplicateKeyException e)
        { 
          for(MongoEntry entry : lst)
          {
            collection.save(entry, WriteConcern.ACKNOWLEDGED);
          }
        }
      }

    }



}
