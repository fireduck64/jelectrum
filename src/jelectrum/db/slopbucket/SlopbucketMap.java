package jelectrum.db.slopbucket;


import com.google.protobuf.ByteString;
import java.util.Map;
import jelectrum.db.DBMap;

import slopbucket.Slopbucket;
import java.util.concurrent.Semaphore;

public class SlopbucketMap extends DBMap
{
  private SlopbucketDB slop_db;
  private String name;
  private Slopbucket slop_fixed;

  public SlopbucketMap(SlopbucketDB slop_db, String name)
  {
    this.slop_db = slop_db;
    this.name = name;
  }

  public ByteString get(String key)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());

    Slopbucket slop = slop_fixed;
    if (slop == null) slop = slop_db.getBucketForKey(key_bytes);

    return slop.getKeyValue(name, key_bytes);

  }
  public void put(String key, ByteString value)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());
    
    Slopbucket slop = slop_fixed;
    if (slop == null) slop = slop_db.getBucketForKey(key_bytes);

    slop.putKeyValue(name, key_bytes, value);
  }

  @Override
  public void putAll(Map<String, ByteString> m)
  {
    final Semaphore sem = new Semaphore(0);
    int count = 0;
    for(Map.Entry<String, ByteString> me : m.entrySet())
    {
      final String key = me.getKey();
      final ByteString value = me.getValue();

      slop_db.getExec().execute(
        new Runnable()
        {
          public void run()
          {
            put(key, value);
            sem.release(1);
          }

        }
        );
      count++;
    }
    try
    {
      sem.acquire(count);
    }
    catch(InterruptedException e)
    {
      throw new RuntimeException(e);
    }

  }

}
