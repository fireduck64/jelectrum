package jelectrum.db.slopbucket;


import com.google.protobuf.ByteString;
import java.util.Map;
import jelectrum.db.DBMap;

import slopbucket.Slopbucket;
import lobstack.ZUtil;
import java.util.concurrent.Semaphore;

public class SlopbucketMap extends DBMap
{
  private SlopbucketDB slop_db;
  private String name;
  private Slopbucket slop_fixed;
  private boolean compressed;

  public SlopbucketMap(SlopbucketDB slop_db, String name, Slopbucket slop_fixed)
  {
    this(slop_db, name, slop_fixed, false);
  }
  public SlopbucketMap(SlopbucketDB slop_db, String name, Slopbucket slop_fixed, boolean compressed)
  {
    this.slop_db = slop_db;
    this.name = name;
    this.compressed = compressed;
    this.slop_fixed = slop_fixed;
  }

  public ByteString get(String key)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());

    Slopbucket slop = slop_fixed;
    //if (slop == null) slop = slop_db.getBucketForKey(key_bytes);

    ByteString value = slop.getKeyValue(name, key_bytes);
    if (value == null) return null;
    if (compressed)
    {
      value = ZUtil.decompress(value);
    }
    return value;

  }
  public void put(String key, ByteString value)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());
    
    Slopbucket slop = slop_fixed;
    //if (slop == null) slop = slop_db.getBucketForKey(key_bytes);

    if (compressed)
    {
      //double old_sz = value.size();
      value = ZUtil.compress(value);
      //double new_sz = value.size();
      //double percent = new_sz / old_sz;
      //slop_db.getLog().log("Compressed: " + key + " " + old_sz + " " + new_sz + " " + percent);
    }

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
