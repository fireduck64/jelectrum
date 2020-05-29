package jelectrum.db.jedis;

import com.google.protobuf.ByteString;
import java.util.Map;

import jelectrum.db.DBMap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

public class JedisMap extends DBMap
{
  private JedisPool pool;
  private String name;

  public JedisMap(JedisPool pool, String name)
  {
    this.pool = pool;
    this.name = name;
  }


  @Override
  public ByteString get(String key)
  {
    String look = name + "/" + key;
    try(Jedis jedis = pool.getResource())
    {
      byte[] val = jedis.get(look.getBytes());

      if (val == null) return null;
      return ByteString.copyFrom(val);
    }

  }

  @Override
  public void put(String key, ByteString value)
  {
    String look = name + "/" + key;

    try(Jedis jedis = pool.getResource())
    {
      jedis.set(look.getBytes(), value.toByteArray());
    }

    

  }

}
