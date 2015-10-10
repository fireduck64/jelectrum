package jelectrum.db;

import jelectrum.TimeRecord;
import com.google.protobuf.ByteString;
import java.util.Map;

public abstract class DBMap
{

  public abstract ByteString get(String key);
  public boolean containsKey(String key)
  {
    return get(key) != null;
  }
  public abstract void put(String key, ByteString value);

  /** Implementing class should override this if they have something better to do */
  public void putAll(Map<String, ByteString> m)
  {
    long t1 = System.nanoTime();
    for(Map.Entry<String, ByteString> me : m.entrySet())
    {
      put(me.getKey(), me.getValue());
    }
    TimeRecord.record(t1, "db_putall_seq");
  }

  
}

