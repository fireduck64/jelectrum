package jelectrum.db.slopbucket;


import com.google.protobuf.ByteString;
import java.util.Map;
import jelectrum.db.DBMap;

import slopbucket.Slopbucket;

public class SlopbucketMap extends DBMap
{
  private SlopbucketDB slop_db;
  private String name;

  public SlopbucketMap(SlopbucketDB slop_db, String name)
  {
    this.slop_db = slop_db;
    this.name = name;
  }

  public ByteString get(String key)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());

    Slopbucket slop = slop_db.getBucketForKey(key_bytes);

    return slop.getKeyValue(name, key_bytes);

  }
  public void put(String key, ByteString value)
  {
    ByteString key_bytes = ByteString.copyFrom(key.getBytes());
    
    Slopbucket slop = slop_db.getBucketForKey(key_bytes);

    slop.putKeyValue(name, key_bytes, value);

  }

}
